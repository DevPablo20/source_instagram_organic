"""Streams do conector. Toda stream derivada de mídia opera dentro de uma janela de dias.

O que distingue este conector do oficial está em três lugares deste arquivo:

- `_normalize`, que transforma `views`/`facebook_views`/`total_views` na métrica `views` em
  três escopos, e deriva o `paid` que fecha a conta;
- as janelas (`lookback_window_days`, `insights_max_age_days`, `account_lookback_days`,
  `comment_window_days`), que impedem o sync de crescer com a idade da conta;
- o pedido em lote, que torna a janela barata o bastante para rodar todo dia.
"""

from __future__ import annotations

import json
from abc import ABC
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from urllib.parse import urlencode

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import IncrementalMixin, Stream

from .api import UNAVAILABLE, GraphAPI, GraphAPIError, InsightsResult

ACCOUNT_INSIGHTS_CHUNK_DAYS = 30        # a API recusa 31 dias entre since e until
MEDIA_PAGE_SIZE = 100

MEDIA_FIELDS = (
    "id,ig_id,shortcode,permalink,timestamp,caption,media_type,media_product_type,"
    "is_comment_enabled,username,owner,alt_text"
)
COUNTER_FIELDS = "id,timestamp,like_count,comments_count"
STORY_FIELDS = "id,timestamp,media_type,media_product_type,permalink,shortcode,caption"
COMMENT_FIELDS = "id,timestamp,text,like_count,hidden,parent_id,username,from{id,username}"
REPLY_FIELDS = "id,timestamp,text,like_count,hidden,parent_id,username,from{id,username}"

# --- listas validadas contra a API; ver docs/metrics.md e docs/streams.md ---

ACCOUNT_SERIES_METRICS = ["reach", "follower_count"]
ACCOUNT_ROLLING_PERIODS = ["week", "days_28"]        # só `reach` aceita
ACCOUNT_TOTAL_METRICS = [
    "views", "likes", "comments", "shares", "saves", "replies", "reposts",
    "total_interactions", "accounts_engaged", "profile_views", "website_clicks",
    "profile_links_taps", "follows_and_unfollows",
]
ACCOUNT_FORMAT_METRICS = [
    "views", "reach", "likes", "comments", "saves", "shares", "total_interactions",
]
ACCOUNT_FOLLOW_TYPE_METRICS = ["reach", "follows_and_unfollows"]
ACCOUNT_CONTACT_METRICS = ["profile_links_taps"]
DEMOGRAPHIC_BREAKDOWNS = ["age", "gender", "country", "city"]

# `facebook_views` e `crossposted_views` ficam fora da lista principal de propósito: elas
# falham em mídia não cruzada e derrubariam o lote inteiro, forçando o isolamento caro.
MEDIA_METRICS_FEED = [
    "reach", "views", "likes", "comments", "saved", "shares", "follows", "profile_visits",
    "total_interactions", "reposts", "total_views", "total_likes", "total_comments",
]
MEDIA_METRICS_REELS = [
    "reach", "views", "likes", "comments", "saved", "shares", "total_interactions",
    "reposts", "ig_reels_avg_watch_time", "ig_reels_video_view_total_time",
    "reels_skip_rate", "total_views", "total_likes", "total_comments",
]
STORY_METRICS = [
    "reach", "views", "replies", "shares", "follows", "profile_visits",
    "total_interactions", "link_clicks", "reposts", "total_views",
]

# Métricas que só vêm com breakdown próprio, num pedido à parte.
PROFILE_ACTIVITY_BREAKDOWN = "action_type"
NAVIGATION_BREAKDOWN = "story_navigation_action_type"

# --- modelo de escopo (invariante 6) ---

SCOPE_ORGANIC = "organic"
SCOPE_FACEBOOK = "facebook"
SCOPE_PAID = "paid"
SCOPE_TOTAL = "total"

# Nome cru da API -> (métrica normalizada, escopo).
RAW_TO_SCOPE: Dict[str, Tuple[str, str]] = {
    "facebook_views": ("views", SCOPE_FACEBOOK),
    "total_views": ("views", SCOPE_TOTAL),
    "total_likes": ("likes", SCOPE_TOTAL),
    "total_comments": ("comments", SCOPE_TOTAL),
}
# Métricas cujo `paid` o conector deriva, e se o Facebook entra na conta.
DERIVABLE = {"views": True, "likes": False, "comments": False}

# A dimensão `AD` do breakdown de formato é o impulsionamento.
AD_DIMENSION = "AD"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _to_unix(moment: datetime) -> int:
    return int(moment.timestamp())


def _parse_ts(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("+0000", "+00:00"))


def _value_pair(value: Any) -> Dict[str, Any]:
    """Métrica escalar vira número; estrutura aninhada vira JSON — nunca os dois."""
    if isinstance(value, bool):
        return {"value_numeric": None, "value_json": json.dumps(value)}
    if isinstance(value, (int, float)):
        return {"value_numeric": value, "value_json": None}
    if value in (None, {}, []):
        return {"value_numeric": None, "value_json": None}
    return {"value_numeric": None, "value_json": json.dumps(value, ensure_ascii=False)}


def _relative(path: str, **params: Any) -> str:
    return f"{path}?{urlencode(params)}"


class InstagramStream(Stream, ABC):
    def __init__(self, api: GraphAPI, config: Mapping[str, Any]):
        self.api = api
        self.config = config
        self.account_id = str(config["instagram_business_account_id"])
        self.start_date = datetime.fromisoformat(str(config["start_date"])).replace(tzinfo=timezone.utc)
        self.lookback_days = int(config.get("lookback_window_days") or 28)
        self.account_lookback_days = int(config.get("account_lookback_days") or 7)
        self.insights_max_age_days = int(config.get("insights_max_age_days") or 700)
        self.comment_window_days = int(config.get("comment_window_days") or 90)
        self.include_replies = bool(config.get("include_replies", True))
        self.demographics_timeframe = config.get("demographics_timeframe") or "last_30_days"
        self._snapshot_date = _utcnow().date().isoformat()
        self._extracted_at = _utcnow().isoformat()
        self._page_id: Optional[str] = None

    @property
    def page_id(self) -> Optional[str]:
        """Junção com o `facebook_organic`. O node do IG não traz o id da página."""
        if self._page_id is None:
            self._page_id = self.api.linked_page_id(self.account_id) or ""
        return self._page_id or None

    def _envelope(self, record: Dict[str, Any], with_snapshot: bool = True) -> Dict[str, Any]:
        record["business_account_id"] = self.account_id
        record["page_id"] = self.page_id
        record["extracted_at"] = self._extracted_at
        if with_snapshot:
            record["snapshot_date"] = self._snapshot_date
        return record

    # ---------- mídia dentro da janela ----------

    def _list_media(self, since: datetime, fields: str) -> List[Dict[str, Any]]:
        return list(self.api.paginate(
            f"{self.account_id}/media", limit=MEDIA_PAGE_SIZE, fields=fields,
            since=_to_unix(since), until=_to_unix(_utcnow()),
        ))


class StateMixin(IncrementalMixin):
    """O state limita o que o conector lê; a gravação é sempre append."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._state: MutableMapping[str, Any] = {}

    @property
    def state(self) -> MutableMapping[str, Any]:
        return self._state

    @state.setter
    def state(self, value: MutableMapping[str, Any]) -> None:
        self._state = dict(value or {})


# =========================== conta ===========================


class Account(InstagramStream):
    """Foto diária da conta: identidade e curva de seguidores."""

    primary_key = ["business_account_id", "snapshot_date"]
    name = "account"

    def read_records(self, sync_mode: SyncMode, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        fields = ("id,username,name,biography,website,followers_count,follows_count,"
                  "media_count,is_published,has_profile_pic")
        data = self.api.get(self.account_id, fields=fields)
        data.pop("id", None)
        yield self._envelope(data)


class AccountInsights(StateMixin, InstagramStream):
    """Métrica × recorte × dia, juntando as cinco formas de pedido que a API exige."""

    primary_key = ["business_account_id", "metric", "period", "scope",
                   "breakdown_key", "breakdown_value", "date"]
    name = "account_insights"
    cursor_field = "date"

    def _window_start(self) -> datetime:
        synced = self._state.get(self.cursor_field)
        if not synced:
            return self.start_date
        previous = datetime.fromisoformat(str(synced)).replace(tzinfo=timezone.utc)
        return max(self.start_date, previous - timedelta(days=self.account_lookback_days))

    def _row(self, metric: str, period: str, scope: str, date: str, value: Any,
             breakdown_key: Optional[str] = None, breakdown_value: Optional[str] = None,
             title: Optional[str] = None) -> Mapping[str, Any]:
        return self._envelope({
            "date": date,
            "metric": metric,
            "period": period,
            "scope": scope,
            "breakdown_key": breakdown_key,
            "breakdown_value": breakdown_value,
            "title": title,
            "is_derived": False,
            **_value_pair(value),
        })

    def _read_series(self, start: datetime, now: datetime) -> Iterable[Mapping[str, Any]]:
        """Série nativa: só `reach` e `follower_count`, fatiada de 30 em 30 dias."""
        for period, metrics in [("day", ACCOUNT_SERIES_METRICS),
                                *[(p, ["reach"]) for p in ACCOUNT_ROLLING_PERIODS]]:
            cursor = start
            while cursor < now:
                chunk_end = min(cursor + timedelta(days=ACCOUNT_INSIGHTS_CHUNK_DAYS), now)
                result = self.api.insights(
                    self.account_id, metrics=metrics, period=period,
                    metric_type="time_series", since=_to_unix(cursor), until=_to_unix(chunk_end),
                )
                for item in result.data:
                    for value in item.get("values", []):
                        end_time = value.get("end_time") or ""
                        yield self._row(item.get("name"), item.get("period"), SCOPE_TOTAL,
                                        end_time[:10] or None, value.get("value"),
                                        title=item.get("title"))
                cursor = chunk_end

    def _day_requests(self, day: datetime) -> List[Tuple[str, Optional[str]]]:
        """As quatro formas de pedido de um dia, com o breakdown que cada uma usa."""
        base = {"period": "day", "metric_type": "total_value",
                "since": _to_unix(day), "until": _to_unix(day + timedelta(days=1))}
        return [
            (_relative(f"{self.account_id}/insights",
                       metric=",".join(ACCOUNT_TOTAL_METRICS), **base), None),
            (_relative(f"{self.account_id}/insights", metric=",".join(ACCOUNT_FORMAT_METRICS),
                       breakdown="media_product_type", **base), "media_product_type"),
            (_relative(f"{self.account_id}/insights",
                       metric=",".join(ACCOUNT_FOLLOW_TYPE_METRICS),
                       breakdown="follow_type", **base), "follow_type"),
            (_relative(f"{self.account_id}/insights", metric=",".join(ACCOUNT_CONTACT_METRICS),
                       breakdown="contact_button_type", **base), "contact_button_type"),
        ]

    def _read_daily(self, start: datetime, now: datetime) -> Iterable[Mapping[str, Any]]:
        """`total_value` é agregado do intervalo: a série diária exige um pedido por dia."""
        days = []
        cursor = start.replace(hour=0, minute=0, second=0, microsecond=0)
        while cursor < now:
            days.append(cursor)
            cursor += timedelta(days=1)

        pedidos: List[str] = []
        contexto: List[Tuple[str, Optional[str]]] = []
        for day in days:
            for url, breakdown in self._day_requests(day):
                pedidos.append(url)
                contexto.append((day.date().isoformat(), breakdown))

        self.logger.info(
            f"account_insights: {len(days)} dias, {len(pedidos)} sub-requests em lote"
        )
        for (body, error), (date, breakdown) in zip(self.api.batch(pedidos), contexto):
            if error:
                if error.invalid_metric:
                    self.logger.warning(f"Pedido recusado em {date} ({breakdown}): {error.message}")
                    continue
                raise error
            for item in body.get("data", []):
                total = item.get("total_value") or {}
                metric, title = item.get("name"), item.get("title")
                for group in total.get("breakdowns") or []:
                    key = ",".join(group.get("dimension_keys") or []) or breakdown
                    for entry in group.get("results", []):
                        dimension = ",".join(entry.get("dimension_values") or [])
                        scope = SCOPE_PAID if dimension == AD_DIMENSION else SCOPE_ORGANIC
                        yield self._row(metric, "day", scope, date, entry.get("value"),
                                        breakdown_key=key, breakdown_value=dimension, title=title)
                if breakdown is None:
                    yield self._row(metric, "day", SCOPE_TOTAL, date, total.get("value"),
                                    title=title)

    def read_records(self, sync_mode: SyncMode, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        now = _utcnow()
        start = self._window_start()
        self.logger.info(f"account_insights: janela de {start.date()} a {now.date()}")
        yield from self._read_series(start, now)
        yield from self._read_daily(start, now)
        self._state = {self.cursor_field: self._snapshot_date}


class AccountDemographics(InstagramStream):
    """Quem segue a conta: idade, gênero, país e cidade."""

    primary_key = ["business_account_id", "metric", "breakdown_key", "breakdown_value",
                   "snapshot_date"]
    name = "account_demographics"

    def read_records(self, sync_mode: SyncMode, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        for breakdown in DEMOGRAPHIC_BREAKDOWNS:
            result = self.api.insights(
                self.account_id, metrics=["follower_demographics"], period="lifetime",
                metric_type="total_value", breakdown=breakdown,
                timeframe=self.demographics_timeframe,
            )
            for item in result.data:
                total = item.get("total_value") or {}
                for group in total.get("breakdowns") or []:
                    key = ",".join(group.get("dimension_keys") or []) or breakdown
                    for entry in group.get("results", []):
                        yield self._envelope({
                            "metric": item.get("name"),
                            "timeframe": self.demographics_timeframe,
                            "breakdown_key": key,
                            "breakdown_value": ",".join(entry.get("dimension_values") or []),
                            **_value_pair(entry.get("value")),
                        })


# =========================== mídia ===========================


class Media(StateMixin, InstagramStream):
    """Append-once: a publicação não muda, então nunca é reextraída."""

    primary_key = "media_id"
    name = "media"
    cursor_field = "timestamp"

    def read_records(self, sync_mode: SyncMode, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        cursor = self._state.get(self.cursor_field)
        since = (_parse_ts(cursor) + timedelta(seconds=1)) if cursor else self.start_date
        latest = cursor
        for media in self._list_media(since, MEDIA_FIELDS):
            timestamp = media.get("timestamp")
            if timestamp and (latest is None or timestamp > latest):
                latest = timestamp
            yield self._envelope({
                "media_id": media.get("id"),
                "ig_id": media.get("ig_id"),
                "shortcode": media.get("shortcode"),
                "permalink": media.get("permalink"),
                "timestamp": timestamp,
                "caption": media.get("caption"),
                "media_type": media.get("media_type"),
                "media_product_type": media.get("media_product_type"),
                "is_comment_enabled": media.get("is_comment_enabled"),
                "username": media.get("username"),
                "owner_id": (media.get("owner") or {}).get("id"),
                "alt_text": media.get("alt_text"),
            }, with_snapshot=False)
        if latest:
            self._state = {self.cursor_field: latest}


class MediaMetrics(StateMixin, InstagramStream):
    """Contadores por snapshot. Sobrevive à retenção que destrói os insights (D27)."""

    primary_key = ["media_id", "snapshot_date"]
    name = "media_metrics"
    cursor_field = "snapshot_date"

    def _window_start(self) -> datetime:
        if not self._state.get(self.cursor_field):
            return self.start_date
        return max(self.start_date, _utcnow() - timedelta(days=self.lookback_days))

    def read_records(self, sync_mode: SyncMode, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        for media in self._list_media(self._window_start(), COUNTER_FIELDS):
            yield self._envelope({
                "media_id": media.get("id"),
                "timestamp": media.get("timestamp"),
                "like_count": media.get("like_count"),
                "comments_count": media.get("comments_count"),
            })
        self._state = {self.cursor_field: self._snapshot_date}


class InsightsScopeMixin:
    """Converte o nome cru da API em métrica + escopo, e deriva o impulsionado.

    `views`/`facebook_views`/`total_views` viram a métrica `views` em três escopos; o quarto,
    `paid`, é o que fecha a conta. Verificado em 81 mídias, sem caso incoerente.
    """

    @staticmethod
    def _normalize(raw_metric: str) -> Tuple[str, str]:
        return RAW_TO_SCOPE.get(raw_metric, (raw_metric, SCOPE_ORGANIC))

    @staticmethod
    def _iter_breakdown(answer: Any, breakdown: str
                        ) -> Iterable[Tuple[str, Optional[str], Optional[str], Any, Optional[str]]]:
        """Desdobra uma resposta com breakdown em (métrica, chave, dimensão, valor, título).

        Devolve também o escalar da métrica, que vem no mesmo `total_value` — pedir de novo
        sem breakdown seria um request a mais para o número que já está na mão.
        """
        if answer is None:
            return
        body, error = answer
        if error:
            return
        for raw in body.get("data", []):
            total = raw.get("total_value") or {}
            title = raw.get("title")
            for group in total.get("breakdowns") or []:
                key = ",".join(group.get("dimension_keys") or []) or breakdown
                for entry in group.get("results", []):
                    yield (raw.get("name"), key,
                           ",".join(entry.get("dimension_values") or []),
                           entry.get("value"), title)
            yield raw.get("name"), None, None, total.get("value"), title

    @staticmethod
    def _derive_paid(scalars: Dict[Tuple[str, str], Any]) -> List[Tuple[str, Any]]:
        """`paid` só é emitido quando todas as parcelas existem; caso contrário, nada."""
        derived: List[Tuple[str, Any]] = []
        for metric, uses_facebook in DERIVABLE.items():
            organic = scalars.get((metric, SCOPE_ORGANIC))
            total = scalars.get((metric, SCOPE_TOTAL))
            if organic is None or total is None:
                continue
            facebook = scalars.get((metric, SCOPE_FACEBOOK)) or 0 if uses_facebook else 0
            derived.append((metric, total - organic - facebook))
        return derived


class MediaInsights(StateMixin, InstagramStream, InsightsScopeMixin):
    """Métrica × mídia × snapshot, com orgânico e impulsionado separados."""

    primary_key = ["media_id", "metric", "scope", "breakdown_key", "breakdown_value",
                   "snapshot_date"]
    name = "media_insights"
    cursor_field = "snapshot_date"

    def _window_start(self) -> datetime:
        requested = (self.start_date if not self._state.get(self.cursor_field)
                     else max(self.start_date, _utcnow() - timedelta(days=self.lookback_days)))
        floor = _utcnow() - timedelta(days=self.insights_max_age_days)
        if requested < floor:
            self.logger.info(
                f"Janela pedida começa em {requested.date()}, antes do teto de "
                f"{self.insights_max_age_days} dias; truncada para {floor.date()} — acima "
                f"disso a plataforma já degradou alcance e visualizações."
            )
            return floor
        return requested

    def _metrics_for(self, media: Mapping[str, Any]) -> List[str]:
        if media.get("media_product_type") == "REELS":
            return self.config.get("media_metrics_reels") or MEDIA_METRICS_REELS
        return self.config.get("media_metrics_feed") or MEDIA_METRICS_FEED

    def _row(self, media: Mapping[str, Any], metric: str, scope: str, value: Any,
             breakdown_key: Optional[str] = None, breakdown_value: Optional[str] = None,
             title: Optional[str] = None, is_derived: bool = False) -> Mapping[str, Any]:
        return self._envelope({
            "media_id": media.get("id"),
            "timestamp": media.get("timestamp"),
            "media_product_type": media.get("media_product_type"),
            "metric": metric,
            "scope": scope,
            "breakdown_key": breakdown_key,
            "breakdown_value": breakdown_value,
            "title": title,
            "is_derived": is_derived,
            **_value_pair(value),
        })

    def read_records(self, sync_mode: SyncMode, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        media = self._list_media(self._window_start(), "id,timestamp,media_product_type")
        self.logger.info(f"media_insights: {len(media)} mídias na janela")
        if not media:
            self._state = {self.cursor_field: self._snapshot_date}
            return

        core = [_relative(f"{m['id']}/insights", metric=",".join(self._metrics_for(m)))
                for m in media]
        # Pedidas à parte porque falham em mídia não cruzada e derrubariam o lote inteiro.
        facebook = [_relative(f"{m['id']}/insights", metric="facebook_views") for m in media]
        crossposted = [_relative(f"{m['id']}/insights", metric="crossposted_views")
                       if m.get("media_product_type") == "REELS" else None for m in media]
        activity = [_relative(f"{m['id']}/insights", metric="profile_activity",
                              breakdown=PROFILE_ACTIVITY_BREAKDOWN, metric_type="total_value")
                    if m.get("media_product_type") != "REELS" else None for m in media]

        core_out = self.api.batch(core)
        facebook_out = self.api.batch(facebook)
        crossposted_out = self._batch_optional(crossposted)
        activity_out = self._batch_optional(activity)

        for item, (body, error), fb, cross, act in zip(
                media, core_out, facebook_out, crossposted_out, activity_out):
            if error:
                # Uma métrica recusada derruba o lote inteiro daquela mídia; refazer fora
                # do lote aciona o isolamento e salva as que respondem.
                self.logger.warning(
                    f"Insights da mídia {item['id']} falharam no lote ({error.message}); "
                    f"tentando sozinha"
                )
                try:
                    body = {"data": self.api.insights(
                        item["id"], metrics=self._metrics_for(item)).data}
                except GraphAPIError as exc:
                    self.logger.warning(f"Mídia {item['id']} ficou sem insights: {exc.message}")
                    continue
            scalars: Dict[Tuple[str, str], Any] = {}
            rows: List[Mapping[str, Any]] = []

            for raw in body.get("data", []):
                metric, scope = self._normalize(raw.get("name"))
                value = (raw.get("values") or [{}])[0].get("value")
                scalars[(metric, scope)] = value
                rows.append(self._row(item, metric, scope, value, title=raw.get("title")))

            rows.extend(self._facebook_rows(item, fb, scalars))
            rows.extend(self._extra_rows(item, cross))
            rows.extend(self._breakdown_rows(item, act, PROFILE_ACTIVITY_BREAKDOWN))

            for metric, value in self._derive_paid(scalars):
                rows.append(self._row(item, metric, SCOPE_PAID, value, is_derived=True))

            yield from rows

        self._state = {self.cursor_field: self._snapshot_date}

    def _batch_optional(self, urls: List[Optional[str]]) -> List[Any]:
        """Mantém o alinhamento posicional quando a métrica não se aplica à mídia."""
        wanted = [u for u in urls if u]
        answers = iter(self.api.batch(wanted)) if wanted else iter(())
        return [next(answers) if u else None for u in urls]

    def _facebook_rows(self, media: Mapping[str, Any], answer: Any,
                       scalars: Dict[Tuple[str, str], Any]) -> List[Mapping[str, Any]]:
        """Subcode 2207086 é zero, não ausente — é o que mantém o `paid` fechado (D28)."""
        if answer is None:
            return []
        body, error = answer
        if error:
            if not error.metric_unavailable:
                return []
            scalars[("views", SCOPE_FACEBOOK)] = 0
            return [self._row(media, "views", SCOPE_FACEBOOK, 0, is_derived=True)]
        rows = []
        for raw in body.get("data", []):
            value = (raw.get("values") or [{}])[0].get("value")
            scalars[("views", SCOPE_FACEBOOK)] = value
            rows.append(self._row(media, "views", SCOPE_FACEBOOK, value, title=raw.get("title")))
        return rows

    def _extra_rows(self, media: Mapping[str, Any], answer: Any) -> List[Mapping[str, Any]]:
        """`crossposted_views` é orgânico nas duas superfícies: Instagram mais Facebook."""
        if answer is None:
            return []
        body, error = answer
        if error:
            return []
        return [self._row(media, raw.get("name"), SCOPE_ORGANIC,
                          (raw.get("values") or [{}])[0].get("value"), title=raw.get("title"))
                for raw in body.get("data", [])]

    def _breakdown_rows(self, media: Mapping[str, Any], answer: Any,
                        breakdown: str) -> List[Mapping[str, Any]]:
        return [self._row(media, metric, SCOPE_ORGANIC, value, breakdown_key=key,
                          breakdown_value=dimension, title=title)
                for metric, key, dimension, value, title
                in self._iter_breakdown(answer, breakdown)]


# =========================== stories ===========================


class Story(InstagramStream):
    """Story some em 24h: é foto do que está no ar, nunca incremental."""

    primary_key = ["story_id", "snapshot_date"]
    name = "story"

    def read_records(self, sync_mode: SyncMode, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        for story in self.api.paginate(f"{self.account_id}/stories", limit=MEDIA_PAGE_SIZE,
                                       fields=STORY_FIELDS):
            yield self._envelope({
                "story_id": story.get("id"),
                "timestamp": story.get("timestamp"),
                "media_type": story.get("media_type"),
                "media_product_type": story.get("media_product_type"),
                "permalink": story.get("permalink"),
                "shortcode": story.get("shortcode"),
                "caption": story.get("caption"),
            })


class StoryInsights(InstagramStream, InsightsScopeMixin):
    """Métrica × story × snapshot. A última leitura antes de expirar é o número final."""

    primary_key = ["story_id", "metric", "scope", "breakdown_key", "breakdown_value",
                   "snapshot_date"]
    name = "story_insights"

    def _row(self, story_id: str, metric: str, scope: str, value: Any,
             breakdown_key: Optional[str] = None, breakdown_value: Optional[str] = None,
             title: Optional[str] = None, is_derived: bool = False) -> Mapping[str, Any]:
        return self._envelope({
            "story_id": story_id,
            "metric": metric,
            "scope": scope,
            "breakdown_key": breakdown_key,
            "breakdown_value": breakdown_value,
            "title": title,
            "is_derived": is_derived,
            **_value_pair(value),
        })

    def read_records(self, sync_mode: SyncMode, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        stories = list(self.api.paginate(f"{self.account_id}/stories", limit=MEDIA_PAGE_SIZE,
                                         fields="id"))
        if not stories:
            self.logger.info("story_insights: nenhuma story no ar")
            return
        metrics = self.config.get("story_metrics") or STORY_METRICS
        self.logger.info(f"story_insights: {len(stories)} stories no ar")

        core = self.api.batch([_relative(f"{s['id']}/insights", metric=",".join(metrics))
                               for s in stories])
        facebook = self.api.batch([_relative(f"{s['id']}/insights", metric="facebook_views")
                                   for s in stories])
        # As duas que só existem com breakdown próprio, cada uma no seu pedido.
        navegacao = self.api.batch([
            _relative(f"{s['id']}/insights", metric="navigation",
                      breakdown=NAVIGATION_BREAKDOWN, metric_type="total_value")
            for s in stories])
        atividade = self.api.batch([
            _relative(f"{s['id']}/insights", metric="profile_activity",
                      breakdown=PROFILE_ACTIVITY_BREAKDOWN, metric_type="total_value")
            for s in stories])

        for story, (body, error), fb, nav, act in zip(
                stories, core, facebook, navegacao, atividade):
            story_id = story["id"]
            if error:
                if error.not_enough_viewers:
                    # A story existe e a métrica não. Não é falha de sync.
                    self.logger.info(f"Story {story_id} sem métrica: {error.message}")
                    continue
                # Story não se relê: ela expira em 24h. Um erro que não seja "poucos
                # espectadores" merece uma segunda tentativa fora do lote — já foi
                # observado `(#200) Permissions error` que some ao repetir.
                self.logger.warning(
                    f"Story {story_id} falhou no lote ({error.message}); tentando sozinha"
                )
                try:
                    body = {"data": self.api.insights(story_id, metrics=metrics).data}
                except GraphAPIError as exc:
                    self.logger.warning(f"Story {story_id} ficou sem métrica: {exc.message}")
                    continue
            scalars: Dict[Tuple[str, str], Any] = {}
            for raw in body.get("data", []):
                metric, scope = self._normalize(raw.get("name"))
                value = (raw.get("values") or [{}])[0].get("value")
                scalars[(metric, scope)] = value
                yield self._row(story_id, metric, scope, value, title=raw.get("title"))

            fb_body, fb_error = fb
            if fb_error is None:
                for raw in fb_body.get("data", []):
                    value = (raw.get("values") or [{}])[0].get("value")
                    scalars[("views", SCOPE_FACEBOOK)] = value
                    yield self._row(story_id, "views", SCOPE_FACEBOOK, value,
                                    title=raw.get("title"))
            elif fb_error.metric_unavailable:
                scalars[("views", SCOPE_FACEBOOK)] = 0
                yield self._row(story_id, "views", SCOPE_FACEBOOK, 0, is_derived=True)

            for answer, breakdown in [(nav, NAVIGATION_BREAKDOWN),
                                      (act, PROFILE_ACTIVITY_BREAKDOWN)]:
                for metric, key, dimension, value, title in self._iter_breakdown(answer, breakdown):
                    yield self._row(story_id, metric, SCOPE_ORGANIC, value,
                                    breakdown_key=key, breakdown_value=dimension, title=title)

            for metric, value in self._derive_paid(scalars):
                yield self._row(story_id, metric, SCOPE_PAID, value, is_derived=True)


# =========================== comentários ===========================


class Comment(StateMixin, InstagramStream):
    """Comentário e resposta na mesma stream, ligados por `parent_id`."""

    primary_key = "comment_id"
    name = "comment"
    cursor_field = "timestamp"

    def _window_start(self) -> datetime:
        if not self._state.get(self.cursor_field):
            return self.start_date
        return max(self.start_date, _utcnow() - timedelta(days=self.comment_window_days))

    def _record(self, comment: Mapping[str, Any], media_id: str,
                parent_id: Optional[str] = None) -> Mapping[str, Any]:
        author = comment.get("from") or {}
        return self._envelope({
            "comment_id": comment.get("id"),
            "media_id": media_id,
            "parent_id": comment.get("parent_id") or parent_id,
            "timestamp": comment.get("timestamp"),
            "text": comment.get("text"),
            "like_count": comment.get("like_count"),
            "hidden": comment.get("hidden"),
            "username": comment.get("username") or author.get("username"),
            "from_id": author.get("id"),
        }, with_snapshot=False)

    def read_records(self, sync_mode: SyncMode, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        totals: Dict[str, int] = dict(self._state.get("media_totals") or {})
        latest = self._state.get(self.cursor_field)
        fields = COMMENT_FIELDS
        if self.include_replies:
            fields = f"{fields},replies.limit(50){{{REPLY_FIELDS}}}"

        skipped = 0
        missing_author = 0
        seen_any = 0
        for media in self._list_media(self._window_start(), COUNTER_FIELDS):
            media_id = media.get("id")
            total = media.get("comments_count") or 0
            if not media_id or not total:
                continue
            # O total vem de graça na listagem: se não mudou, a mídia não é paginada.
            if totals.get(media_id) == total:
                skipped += 1
                continue

            found = 0
            for comment in self.api.paginate(f"{media_id}/comments", limit=50, fields=fields):
                found += 1
                for record in [self._record(comment, media_id),
                               *[self._record(reply, media_id, parent_id=comment.get("id"))
                                 for reply in (comment.get("replies") or {}).get("data", [])]]:
                    if not record["username"]:
                        missing_author += 1
                    seen_any += 1
                    timestamp = record["timestamp"]
                    if timestamp and (latest is None or timestamp > latest):
                        latest = timestamp
                    yield record

            if found:
                totals[media_id] = total
            else:
                self.logger.warning(
                    f"A mídia {media_id} tem {total} comentários no contador e nenhum "
                    f"conteúdo: confira as permissões do token."
                )

        self.logger.info(f"comment: {skipped} mídias puladas pelo gate de comments_count")
        if seen_any and missing_author == seen_any:
            self.logger.warning(
                f"Os {seen_any} comentários lidos vieram sem autor. Ligue a task 'Atividade "
                f"da comunidade' (MODERATE) na página ligada a esta conta."
            )
        self._state = {self.cursor_field: latest, "media_totals": totals}
