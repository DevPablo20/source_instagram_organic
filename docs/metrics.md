# Métricas de insights — o que a API devolve hoje

Lista **descoberta contra a API**, não copiada de documentação: cada nome foi pedido
individualmente, em conta real e em mídia de cada tipo, e classificado pela resposta.

> **`impressions` morreu; o substituto é `views`.** Em conta, `impressions` é rejeitada com
> `#100` e a mensagem **enumera** as válidas da família legada. Em mídia, a mensagem é
> explícita: *"Starting from version v22.0 and above, the impressions metric is no longer
> supported"*.
>
> | Conceito antigo | Nome vivo hoje |
> |---|---|
> | `impressions` de conta | `views` (só `metric_type=total_value`) |
> | `impressions` de mídia | `views` |
> | `impressions` de story | `views` |

## Conta — `/{ig-user-id}/insights`

A API divide as métricas de conta em dois mundos que **não se misturam num request**.

### Série diária nativa — `metric_type=time_series`

Só duas métricas aceitam série temporal. Devolvem `values[]` com `value` e `end_time`, um
ponto por dia, no máximo 30 dias por request.

| Métrica | Significado (título da API) |
|---|---|
| `reach` | Contas alcançadas |
| `follower_count` | novos seguidores no dia |

`website_clicks`, `profile_views`, `online_followers`, `views`, `likes` e
`accounts_engaged` são **incompatíveis com `time_series`** (`The following metric (x) is
incompatible with the metric type (time_series)`). `online_followers` também é incompatível
com `period=day`.

### Agregado do intervalo — `metric_type=total_value`

Devolvem **um número para todo o intervalo pedido**, não uma série. A série diária se obtém
pedindo um dia por request; as 13 cabem num único request.

| Métrica | Significado (título da API) | Breakdowns que respondem |
|---|---|---|
| `views` | Visualizações | `media_product_type` |
| `reach` | Contas alcançadas | `media_product_type`, `follow_type` |
| `likes` | Gostos | `media_product_type` |
| `comments` | Comentários | `media_product_type` |
| `saves` | Guardados | `media_product_type` |
| `shares` | Partilhas | `media_product_type` |
| `replies` | Respostas | — |
| `reposts` | Republicações | — |
| `total_interactions` | Interações com conteúdos | `media_product_type` |
| `accounts_engaged` | Contas que interagiram | — |
| `profile_views` | Visitas ao perfil | — |
| `website_clicks` | Toques na ligação para o site | — |
| `profile_links_taps` | Toques nas ligações do perfil | `contact_button_type` |
| `follows_and_unfollows` | Seguimentos e cancelamentos | `follow_type` (**obrigatório**) |

`follows_and_unfollows` devolve `total_value: null` **sem** breakdown: só tem valor com
`breakdown=follow_type`, que separa `FOLLOWER` de `NON_FOLLOWER`.

### Rejeitadas em conta

`impressions`, `email_contacts`, `phone_call_clicks`, `text_message_clicks`,
`get_directions_clicks`, `profile_visits` (existe em mídia, não em conta), `threads_views`
(incompatível com `total_value`), `content_views` (aceita, devolve vazio).

### O recorte que o conector oficial não pede

`breakdown=media_product_type` separa **`AD`** de `POST`, `REEL` e `STORY`. É assim que o
orgânico da conta se separa do pago — exatamente o que o plano do `binder_etl` listou como
fora de escopo ("o `reach` do `user_insights` inclui anúncios, e o conector não pede o
breakdown"). Medido na Texaco, 25 dias: `views` total 2.243.411, dos quais `AD` 1.086.168 —
**48% do alcance da conta é pago** e hoje entra no lake como se fosse orgânico.

## Demografia — `period=lifetime`, `timeframe` obrigatório

| Métrica | Breakdowns | Timeframes que respondem |
|---|---|---|
| `follower_demographics` | `age`, `gender`, `country`, `city` | todos: `last_14_days`, `last_30_days`, `last_90_days`, `prev_month`, `this_month`, `this_week` |
| `engaged_audience_demographics` | `age`, `gender`, `country`, `city` | só `this_week` e `this_month` |
| `reached_audience_demographics` | `age`, `gender`, `country`, `city` | só `this_week` e `this_month` |

Os timeframes longos foram desligados para as duas últimas: *"The timeframe parameter
specified last_30_days is no longer supported for engaged_audience_demographics"*. `country`
e `city` vêm limitados aos **45 maiores**; `age` devolve 7 faixas e `gender` 3 valores
(`F`, `M`, `U`).

## Mídia — `/{media-id}/insights`

A lista depende do `media_product_type`. Pedir a lista errada **derruba o request inteiro**.

### `FEED` — imagem e carrossel (15 métricas)

| Métrica | Significado (título da API) |
|---|---|
| `reach` | Contas alcançadas |
| `views` | Visualizações — **responde em imagem e carrossel** |
| `likes` | Gostos (orgânico) |
| `comments` | Comentários (orgânico) |
| `saved` | Guardados |
| `shares` | Partilhas |
| `follows` | Seguidores ganhos pela publicação |
| `profile_visits` | Visitas ao perfil |
| `profile_activity` | Atividade do perfil — breakdown `action_type` |
| `total_interactions` | Interações com a publicação |
| `reposts` | Republicações |
| `facebook_views` | Visualizações no Facebook |
| `total_likes` | Número total de Gostos — **inclui anúncio** |
| `total_comments` | Total de comentários — **inclui anúncio** |
| `total_views` | Total de visualizações — **inclui anúncio e Facebook** |

### `REELS` (16 métricas)

| Métrica | Significado (título da API) |
|---|---|
| `reach`, `views`, `likes`, `comments`, `saved`, `shares`, `total_interactions`, `reposts` | como em `FEED` |
| `ig_reels_avg_watch_time` | Tempo médio de visualização dos reels (ms) |
| `ig_reels_video_view_total_time` | Tempo total de visualização (ms), inclui repetições |
| `reels_skip_rate` | Taxa de perdas — percentual que sai nos 3 primeiros segundos |
| `crossposted_views` | Visualizações das publicações cruzadas (Instagram + Facebook) |
| `facebook_views` | Visualizações no Facebook |
| `total_likes`, `total_comments`, `total_views` | incluem anúncio |

`REELS` **recusa** `follows`, `profile_visits` e `profile_activity`
(*"The Media Insights API does not support the follows metric for this media product
type"*). `FEED` **recusa** `ig_reels_avg_watch_time`, `ig_reels_video_view_total_time`,
`reels_skip_rate` e `crossposted_views`.

### A armadilha do reel não cruzado

`crossposted_views` e `facebook_views` falham com **HTTP 400, `code -1`, mensagem `Fatal`,
`error_subcode: 2207086`, `is_transient: false`** quando o reel não foi publicado cruzado no
Facebook. A própria API explica no `error_user_msg`: *"Apresentado se crossposted_views ou
facebook_views for usado em reels que não são publicações cruzadas no Facebook."*

Como a falha é determinística (5 tentativas, 5 falhas) e derruba o lote inteiro de métricas,
as duas precisam ser pedidas em request separado ou isoladas no fallback. Medido: 11 de 32
reels da janela de 28 dias caem nisso.

### Rejeitadas em mídia

`impressions`, `plays`, `clips_replays_count`, `ig_reels_aggregated_all_plays_count`,
`video_views`, `taps_forward`, `taps_back`, `exits`, `peak_concurrent_viewers`, `thumbstops`,
`average_view_time`, `threads_replies`, `threads_views`, `navigation` (só story),
`replies` (só story), `link_clicks` (só story).

## Story — `/{story-id}/insights` (13 métricas)

| Métrica | Significado (título da API) |
|---|---|
| `reach` | Contas alcançadas |
| `views` | Visualizações |
| `replies` | Respostas — **devolve 0 para criadores europeus e japoneses** |
| `shares` | Partilhas |
| `follows` | Seguidores |
| `profile_visits` | Visitas ao perfil |
| `profile_activity` | Atividade do perfil — breakdown `action_type` |
| `total_interactions` | Interações com a história |
| `navigation` | Navegação — breakdown `story_navigation_action_type` |
| `link_clicks` | Cliques em ligações |
| `reposts` | Republicações |
| `facebook_views` | Visualizações no Facebook |
| `total_views` | Total de visualizações |

O conector oficial pede 7 (`reach`, `views`, `shares`, `follows`, `replies`,
`profile_visits`, `total_interactions`). **Faltam 6**, incluindo `navigation` e
`link_clicks`, que são as que dizem se a story funcionou.

Breakdowns: `story_navigation_action_type` → `SWIPE_FORWARD`, `TAP_BACK`, `TAP_EXIT`,
`TAP_FORWARD`; `action_type` → `BIO_LINK_CLICKED`, `CALL`, `DIRECTION`, `EMAIL`, `OTHER`,
`TEXT`.

**Métrica de story com menos de 5 espectadores devolve `(#10) Not enough viewers for the
media to show insights`** — a story existe e a métrica não.

## O que a produção extrai hoje

O conector oficial grava, por mídia: `reach`, `views`, `saved`, `shares`, `follows`,
`profile_visits`, `ig_reels_avg_watch_time`, `ig_reels_video_view_total_time`, `likes` e
`comments` — e pede `views` **só para reels**, de modo que a visualização de imagem e
carrossel não existe no lake (está documentado como fora de escopo no plano do `binder_etl`).
Na conta, grava `reach`, `reach_week`, `reach_days_28`, `follower_count` e
`online_followers` — nenhuma das 13 métricas `total_value`.

## Leitura dos resultados

- **Todo valor de mídia e de story é `lifetime`** — é por isso que a mesma mídia muda de
  número a cada leitura e por isso existe `snapshot_date`.
- **O par orgânico × total é nativo da API**, não um cálculo: `views`/`total_views`,
  `likes`/`total_likes`, `comments`/`total_comments`. `total_*` só existe com token de
  Facebook Login, que é o nosso caso.
- **`like_count` da listagem de `/media` e `likes` do `/insights` não são o mesmo número.**
  Medido num post de 2024: `like_count` 140 e `likes` 140 coincidem hoje, mas `like_count`
  sobrevive à retenção de 2 anos e `likes`/`reach` não — ver a próxima nota.
- **A retenção de ~2 anos destrói insight, não contador.** Medido na Texaco, imagem de feed,
  uma por faixa de 90 dias: até 632 dias de idade o `reach` fica na ordem de milhares; em
  720 dias cai para 239; em 811 dias para 31; acima de ~900 dias `views` devolve **0** e o
  `reach` fica entre 2 e 21. `like_count` e `likes` permanecem íntegros em todas as faixas.
  É a justificativa do `insights_max_age_days`.
- **`reels_skip_rate` é percentual (55,9), não contagem** — tratar como número fracionário.
- **`navigation` e `profile_activity` sem breakdown devolvem o escalar somado**; o detalhe só
  vem com `breakdown`, dentro de `total_value.breakdowns[].results[]`.
