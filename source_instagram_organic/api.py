"""Cliente da Graph API: paginação, batch, backoff por header de uso e classificação de erro.

Duas diferenças em relação ao cliente do `facebook_organic`, as duas medidas contra a API:

- **Não existe token a derivar.** O token de System User responde em todas as edges do
  Instagram, então não há o passo de pedir o Page Access Token por objeto.
- **O sinal de retry é `is_transient`, não o status HTTP.** A Graph API devolve
  `code -1 / "Fatal"` com **HTTP 400** e `is_transient: false` para dizer "esta métrica não
  existe para este objeto". Repetir isso oito vezes por mídia queima o sync.
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import requests

GRAPH_HOST = "https://graph.facebook.com"

# Códigos que a Meta usa para limite de uso; qualquer outro erro sobe.
RATE_LIMIT_CODES = {4, 17, 32, 613, 80001, 80002, 80003, 80004}
INVALID_METRIC_CODE = 100
PERMISSION_CODE = 10
# `(#200) Permissions error` aparece por métrica, não por objeto: medido numa story em que
# `total_views` falha e as outras 12 respondem. Isolar é a única forma de não perder as 12.
PERMISSION_ERROR_CODE = 200

# `facebook_views` e `crossposted_views` em mídia que não foi publicação cruzada.
# Não é dado faltando: é zero. Ver D28.
METRIC_UNAVAILABLE_SUBCODE = 2207086
NOT_ENOUGH_VIEWERS_HINT = "Not enough viewers"

TRANSIENT_STATUSES = {429, 500, 502, 503, 504}
OVERSIZED_HINT = "reduce the amount of data"
MIN_PAGE_SIZE = 5

MAX_RETRIES = 8
BATCH_SIZE = 50

# Motivos pelos quais uma métrica pedida não voltou. O chamador decide o que fazer com cada
# um: `metric_unavailable` vira zero, os outros viram ausência.
UNAVAILABLE = "metric_unavailable"
INVALID_METRIC = "invalid_metric"
NOT_ENOUGH_VIEWERS = "not_enough_viewers"
PERMISSION_DENIED = "permission_denied"


def redact(text: Any) -> str:
    """Nenhum token sai em log, mensagem de erro ou traceback."""
    return re.sub(r"(access_token=)[^&\s]+", r"\1<redacted>", str(text))


class GraphAPIError(Exception):
    def __init__(self, error: Mapping[str, Any], context: str = ""):
        self.code = error.get("code")
        self.subcode = error.get("error_subcode")
        self.is_transient = error.get("is_transient")
        self.message = redact(error.get("message", ""))
        self.user_message = redact(error.get("error_user_msg", "") or "")
        super().__init__(
            redact(f"{context}{self.message} (code={self.code} subcode={self.subcode})")
        )

    @property
    def metric_unavailable(self) -> bool:
        """A métrica não existe para este objeto — resposta definitiva, não falha."""
        return self.subcode == METRIC_UNAVAILABLE_SUBCODE

    @property
    def invalid_metric(self) -> bool:
        return self.code == INVALID_METRIC_CODE

    @property
    def not_enough_viewers(self) -> bool:
        """Story com menos de 5 espectadores: a story existe, a métrica não."""
        return self.code == PERMISSION_CODE and NOT_ENOUGH_VIEWERS_HINT in self.message

    @property
    def permission_error(self) -> bool:
        return self.code == PERMISSION_ERROR_CODE


@dataclass
class InsightsResult:
    """O que a API devolveu e, nomeadamente, o que ela recusou.

    `unavailable` é o que permite ao stream distinguir "não houve reprodução no Facebook"
    (zero) de "a métrica não respondeu" (ausente).
    """

    data: List[Dict[str, Any]] = field(default_factory=list)
    unavailable: Dict[str, str] = field(default_factory=dict)


class GraphAPI:
    """Toda chamada à Graph API passa por aqui. Nenhum token sai em registro."""

    def __init__(self, access_token: str, api_version: str,
                 logger: Optional[logging.Logger] = None):
        self._token = access_token
        self.api_version = api_version
        self.base = f"{GRAPH_HOST}/{api_version}"
        self.logger = logger or logging.getLogger("airbyte")
        self.session = requests.Session()
        self._page_by_account: Dict[str, Optional[str]] = {}

    # ---------- contas ----------

    def linked_page_id(self, account_id: str) -> Optional[str]:
        """A página ligada à conta, para o `page_id` que junta com o `facebook_organic`.

        O node do IG User não tem o id da página: a ligação só aparece do lado da página,
        em `/me/accounts`. Uma chamada por sync, guardada em memória.
        """
        if account_id in self._page_by_account:
            return self._page_by_account[account_id]
        for page in self.paginate("me/accounts", limit=100,
                                  fields="id,instagram_business_account{id}"):
            linked = (page.get("instagram_business_account") or {}).get("id")
            if linked:
                self._page_by_account.setdefault(linked, page.get("id"))
        found = self._page_by_account.setdefault(account_id, None)
        if not found:
            self.logger.warning(
                f"A conta {account_id} não aparece ligada a nenhuma página alcançada pelo "
                f"token; page_id ficará nulo nos registros."
            )
        return found

    # ---------- transporte ----------

    def _sleep_for(self, response: Optional[requests.Response], attempt: int) -> float:
        """Backoff lendo o header de uso; só cai no exponencial quando o header não ajuda.

        No Instagram o header traz `type: instagram`, com uma entrada pelo app e uma pela
        conta — diferente do Facebook, onde é uma por página.
        """
        wait = min(2 ** attempt, 60)
        if response is not None:
            raw = response.headers.get("x-business-use-case-usage")
            if raw:
                try:
                    for entries in json.loads(raw).values():
                        for entry in entries:
                            estimated = entry.get("estimated_time_to_regain_access") or 0
                            if estimated:
                                wait = max(wait, min(float(estimated) * 60, 600))
                except (ValueError, AttributeError):
                    pass
        return wait

    @staticmethod
    def _halve_page_size(url: str, payload: Dict[str, Any]) -> Optional[Tuple[str, Dict[str, Any]]]:
        """A Graph API recusa a página quando ela é grande demais para a edge.

        A mensagem é "reduce the amount of data". Em vez de falhar, o cliente reduz o
        tamanho da página — inclusive em URL de paginação, onde o `limit` vem embutido.
        """
        parsed = urlparse(url)
        query = dict(parse_qsl(parsed.query))
        if "limit" in payload:
            current = int(payload["limit"])
            if current <= MIN_PAGE_SIZE:
                return None
            return url, {**payload, "limit": max(MIN_PAGE_SIZE, current // 2)}
        if "limit" in query:
            current = int(query["limit"])
            if current <= MIN_PAGE_SIZE:
                return None
            query["limit"] = str(max(MIN_PAGE_SIZE, current // 2))
            return urlunparse(parsed._replace(query=urlencode(query))), payload
        return None

    def _request(self, method: str, path: str,
                 params: Optional[Mapping[str, Any]] = None,
                 data: Optional[Mapping[str, Any]] = None) -> Any:
        url = path if path.startswith("http") else f"{self.base}/{path}"
        payload = dict(params or {})
        payload["access_token"] = self._token
        last_error: Optional[Mapping[str, Any]] = None

        for attempt in range(MAX_RETRIES):
            try:
                if method == "GET":
                    response = self.session.get(url, params=payload, timeout=120)
                else:
                    response = self.session.post(url, data={**payload, **(data or {})}, timeout=180)
            except requests.RequestException as exc:
                wait = min(2 ** attempt, 60)
                self.logger.warning(f"Falha de rede ({redact(exc)}); nova tentativa em {wait}s")
                time.sleep(wait)
                continue

            if response.status_code == 200:
                return response.json()

            try:
                last_error = response.json().get("error", {})
            except ValueError:
                last_error = {"message": response.text[:500], "code": response.status_code}

            code = last_error.get("code")
            transient = last_error.get("is_transient")

            if OVERSIZED_HINT in last_error.get("message", ""):
                smaller = self._halve_page_size(url, payload)
                if smaller:
                    url, payload = smaller
                    size = payload.get("limit") or dict(parse_qsl(urlparse(url).query)).get("limit")
                    self.logger.info(
                        f"Página grande demais em {redact(path)}; reduzindo limit para {size}"
                    )
                    continue

            if code in RATE_LIMIT_CODES:
                wait = self._sleep_for(response, attempt)
                self.logger.warning(
                    f"Limite de uso em {redact(path)} (code={code}); aguardando {wait:.0f}s"
                )
                time.sleep(wait)
                continue

            # A API foi explícita: não adianta repetir. É o caso do subcode 2207086.
            if transient is False:
                raise GraphAPIError(last_error, context=f"{redact(path)}: ")

            if transient is True or response.status_code in TRANSIENT_STATUSES:
                wait = min(2 ** attempt, 60)
                self.logger.warning(
                    f"Erro transitório {response.status_code} (code={code}) em {redact(path)}; "
                    f"tentativa {attempt + 1} de {MAX_RETRIES} em {wait:.0f}s"
                )
                time.sleep(wait)
                continue

            raise GraphAPIError(last_error, context=f"{redact(path)}: ")

        raise GraphAPIError(
            last_error or {"message": "esgotadas as tentativas"}, context=f"{redact(path)}: "
        )

    # ---------- leitura ----------

    def get(self, path: str, **params: Any) -> Dict[str, Any]:
        return self._request("GET", path, params=params)

    def paginate(self, path: str, max_pages: int = 1000, **params: Any) -> Iterable[Dict[str, Any]]:
        """Segue o cursor `next`, devolvendo registro a registro.

        O chamador pode interromper a iteração ao sair da janela: `/media` vem do mais
        recente para o mais antigo.
        """
        page = self._request("GET", path, params=params)
        pages = 0
        while True:
            for record in page.get("data", []):
                yield record
            next_url = (page.get("paging") or {}).get("next")
            pages += 1
            if not next_url or pages >= max_pages:
                return
            page = self._request("GET", next_url)

    def batch(self, relative_urls: List[str]
              ) -> List[Tuple[Optional[Dict[str, Any]], Optional[GraphAPIError]]]:
        """Até 50 sub-requests por chamada, na ordem em que foram pedidos.

        Medido: 50 mídias em 5,5s contra 36s uma a uma. O erro vem por sub-request, então
        uma métrica indisponível numa mídia não derruba as outras 49.
        """
        results: List[Tuple[Optional[Dict[str, Any]], Optional[GraphAPIError]]] = []
        for start in range(0, len(relative_urls), BATCH_SIZE):
            chunk = relative_urls[start:start + BATCH_SIZE]
            payload = json.dumps([{"method": "GET", "relative_url": url} for url in chunk])
            raw = self._request("POST", "", data={"batch": payload})
            items = raw if isinstance(raw, list) else []
            for position, item in enumerate(items):
                if not item:
                    results.append((None, GraphAPIError({"message": "sub-request sem resposta"})))
                    continue
                try:
                    body = json.loads(item["body"])
                except (ValueError, KeyError, TypeError):
                    results.append((None, GraphAPIError({"message": "corpo de sub-request inválido"})))
                    continue
                if item.get("code") == 200:
                    results.append((body, None))
                else:
                    context = f"{redact(chunk[position])}: " if position < len(chunk) else ""
                    results.append((None, GraphAPIError(body.get("error", {}), context=context)))
            # Resposta curta não pode desalinhar as posições seguintes.
            for _ in range(len(chunk) - len(items)):
                results.append((None, GraphAPIError({"message": "sub-request ausente na resposta"})))
        return results

    # ---------- insights ----------

    @staticmethod
    def _reason(exc: GraphAPIError) -> Optional[str]:
        if exc.metric_unavailable:
            return UNAVAILABLE
        if exc.not_enough_viewers:
            return NOT_ENOUGH_VIEWERS
        if exc.invalid_metric:
            return INVALID_METRIC
        if exc.permission_error:
            return PERMISSION_DENIED
        return None

    def insights(self, object_id: str, metrics: List[str], edge: str = "insights",
                 **params: Any) -> InsightsResult:
        """Pede a lista em lote; se a API recusar, isola e classifica a métrica recusada.

        Uma métrica inválida derruba o request inteiro e a mensagem não diz qual é — por
        isso o isolamento, feito uma vez só por lista.
        """
        try:
            data = self.get(f"{object_id}/{edge}", metric=",".join(metrics), **params).get("data", [])
            return InsightsResult(data=data)
        except GraphAPIError as exc:
            if len(metrics) == 1:
                reason = self._reason(exc)
                if reason is None:
                    raise
                if reason in (INVALID_METRIC, PERMISSION_DENIED):
                    self.logger.warning(
                        f"Métrica recusada e descartada: {metrics[0]} ({exc.message})"
                    )
                return InsightsResult(unavailable={metrics[0]: reason})
            if self._reason(exc) is None:
                raise

        merged = InsightsResult()
        for metric in metrics:
            one = self.insights(object_id, [metric], edge=edge, **params)
            merged.data.extend(one.data)
            merged.unavailable.update(one.unavailable)
        return merged
