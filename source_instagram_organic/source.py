"""Ponto de entrada do conector."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, List, Mapping, Optional, Tuple

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .api import GraphAPI, GraphAPIError
from .streams import (
    Account, AccountDemographics, AccountInsights, Comment, Media, MediaInsights,
    MediaMetrics, Story, StoryInsights,
)

DEFAULT_API_VERSION = "v26.0"


class SourceInstagramOrganic(AbstractSource):
    @staticmethod
    def _client(config: Mapping[str, Any], logger: Optional[logging.Logger] = None) -> GraphAPI:
        return GraphAPI(
            access_token=config["access_token"],
            api_version=config.get("api_version") or DEFAULT_API_VERSION,
            logger=logger,
        )

    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        try:
            datetime.fromisoformat(str(config["start_date"])).replace(tzinfo=timezone.utc)
        except ValueError:
            return False, "start_date precisa estar no formato YYYY-MM-DD."

        account_id = str(config["instagram_business_account_id"])
        api = self._client(config, logger)
        try:
            account = api.get(account_id, fields="id,username,followers_count")
            logger.info(
                f"Conectado à conta @{account.get('username')} ({account.get('id')}), "
                f"{account.get('followers_count')} seguidores"
            )

            page_id = api.linked_page_id(account_id)
            if page_id:
                logger.info(f"Conta ligada à página {page_id}")

            # Insights é permissão separada de leitura: confirma antes de o sync começar.
            api.insights(account_id, metrics=["reach"], period="day", metric_type="time_series")

            self._warn_if_author_is_missing(api, account_id, logger)
        except GraphAPIError as exc:
            return False, str(exc)
        return True, None

    @staticmethod
    def _warn_if_author_is_missing(api: GraphAPI, account_id: str,
                                   logger: logging.Logger) -> None:
        """Comentário sem autor é falta de task na página, e a API não diz isso.

        Com `instagram_manage_comments` mas sem a task MODERATE, a resposta vem 200 e os
        campos de autor simplesmente não aparecem. Avisar aqui evita descobrir isso depois,
        olhando uma coluna vazia no lake.
        """
        media = api.get(f"{account_id}/media", limit=25, fields="id,comments_count").get("data", [])
        with_comments = next((m for m in media if m.get("comments_count")), None)
        if not with_comments:
            return
        sample = api.get(f"{with_comments['id']}/comments", limit=1,
                         fields="id,text,username,from{id,username}").get("data", [])
        if not sample:
            logger.warning(
                f"A conta {account_id} devolve contagem de comentários mas nenhum conteúdo. "
                f"Confira as permissões do token."
            )
            return
        if not (sample[0].get("username") or sample[0].get("from")):
            logger.warning(
                f"Os comentários da conta {account_id} vêm sem autor. Ligue a task "
                f"'Atividade da comunidade' (MODERATE) do usuário de sistema na página "
                f"ligada a esta conta; até lá username e from_id ficam nulos na stream "
                f"comment, sem erro."
            )

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        api = self._client(config)
        return [
            Account(api, config),
            AccountInsights(api, config),
            AccountDemographics(api, config),
            Media(api, config),
            MediaMetrics(api, config),
            MediaInsights(api, config),
            Story(api, config),
            StoryInsights(api, config),
            Comment(api, config),
        ]
