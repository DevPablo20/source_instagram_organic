# Contrato das streams — campos, métricas e escopo

O que cada stream entrega, com a lista de métricas fechada por medição contra a API. Este é
o documento que o bronze do `binder_etl` lê para montar os schemas explícitos.

Convenção comum: **toda** stream carrega `business_account_id`, `page_id`, `extracted_at` e,
quando é foto de algo que muda, `snapshot_date`.

## O modelo de escopo: orgânico, Facebook, impulsionado e total

Esta é a decisão central do conector, e a razão principal para não usar o oficial. Toda
stream de insight tem uma coluna **`scope`**, e a pergunta "quanto disso foi impulsionado?"
vira um filtro, não uma fórmula que a equipe de monitoramento precisa lembrar.

| `scope` | Significa | Vem de |
|---|---|---|
| `organic` | alcance e interação espontâneos no Instagram | métrica base (`views`, `likes`, …) na mídia; dimensão ≠ `AD` na conta |
| `facebook` | reprodução no Facebook, por publicação cruzada | `facebook_views` |
| `paid` | impulsionamento | dimensão `AD` na conta; derivado na mídia |
| `total` | tudo somado — é o número que o Business Suite mostra | `total_views`/`total_likes`/`total_comments` na mídia; métrica sem breakdown na conta |

### Na conta, o recorte é nativo

`breakdown=media_product_type` devolve as dimensões `POST`, `REEL`, `CAROUSEL_CONTAINER`,
`STORY` e **`AD`**. O conector mapeia `AD` → `scope: paid` e as demais → `scope: organic`,
preservando a dimensão original em `breakdown_value` para quem quiser o detalhe por formato.

**A soma dos breakdowns é exata** para `views`, `likes`, `comments`, `saves`, `shares` e
`total_interactions` — conferido contra a métrica sem breakdown, diferença zero.

### Na mídia, o recorte é derivado — e fecha

A API não tem dimensão `AD` por mídia, mas tem o par orgânico × total. A decomposição foi
verificada em 81 mídias, **sem um único caso incoerente**:

```
paid_views = total_views − views − facebook_views
```

Para `likes` e `comments` não existe `facebook_likes`, então o derivado absorve o Facebook
junto com o anúncio:

```
paid_likes    = total_likes    − likes        (inclui o Facebook, que não é separável)
paid_comments = total_comments − comments     (idem)
```

Linhas derivadas vêm com **`is_derived: true`**, para o ETL distinguir o que a API mediu do
que o conector calculou.

### Quais métricas têm cada escopo

| Métrica de mídia | `organic` | `facebook` | `paid` | `total` |
|---|---|---|---|---|
| `views` | `views` | `facebook_views` | derivado, exato | `total_views` |
| `likes` | `likes` | — | derivado, absorve FB | `total_likes` |
| `comments` | `comments` | — | derivado, absorve FB | `total_comments` |
| todas as outras | só `organic` | — | — | — |

`reach`, `saved`, `shares`, `follows`, `profile_visits`, `profile_activity`, `reposts` e a
família de reels **só existem em orgânico**. Não há par total; inventar um seria mentira.

### Três regras que o ETL não pode violar

1. **`reach` não soma — nem entre recortes, nem entre dias.** É conta única.
   - Entre recortes: soma 187.850 contra total 186.668 (+1.182), porque a mesma pessoa
     alcançada por um reel e por um anúncio conta uma vez no total e duas na soma. Use a
     linha `scope: total`; `organic + paid` é erro.
   - Entre dias: somar 28 dias dá 1.597.665 contra 1.199.023 reais — **inflação de 1,33×**.
     Alcance de 7 e de 28 dias vem pronto em `period=week` e `period=days_28`.

   Todas as outras métricas **são** aditivas por dia: `views` somado em 28 dias bate com o
   período pedido de uma vez, razão 1,0000.
2. **Valor pode ser negativo.** `saves` é líquido de "desguardar": a dimensão `AD` devolveu
   **−1** numa medição. O schema aceita negativo e o ETL não pode tratar como erro.
3. **Erro `2207086` significa zero, não ausente.** `facebook_views` falha com
   `code -1 / Fatal / subcode 2207086` quando a mídia não foi publicação cruzada — ou seja,
   não houve reprodução no Facebook. O conector grava `facebook_views = 0` com
   `is_derived: true`, o que mantém a decomposição fechada. Tratar como nulo quebraria o
   cálculo de `paid` em 30 das 81 mídias medidas.

---

## `account` — foto diária da conta

`GET /{ig-user-id}` · Full refresh + Append · chave `business_account_id + snapshot_date`

| Campo | Observação |
|---|---|
| `username`, `name`, `biography`, `website` | identificação |
| `followers_count`, `follows_count`, `media_count` | contadores; `media_count` não bate com o total de `/media` (ver P7) |
| `is_published`, `has_profile_pic` | estado |

Fora: `profile_picture_url` (URL de CDN que expira).

## `account_insights` — métrica × recorte × dia

`GET /{ig-user-id}/insights` · Incremental + Append · cursor `date`
Chave: `business_account_id + metric + period + scope + breakdown_key + breakdown_value + date`

Colunas: `date`, `metric`, **`period`**, `scope`, `breakdown_key`, `breakdown_value`,
`value_numeric`, `value_json`, `is_derived`, `title`.

A stream junta **cinco formas de pedido** na mesma tabela, porque o grão é o mesmo.

**1. Série nativa** — `metric_type=time_series`, fatiada de 30 em 30 dias (a API recusa 31).
Só estas duas aceitam série; devolvem um ponto por dia e saem com `scope: total`.

| Métrica | `period` | Significado |
|---|---|---|
| `reach` | `day` | contas alcançadas no dia |
| `follower_count` | `day` | novos seguidores no dia |

**1b. Alcance móvel, já deduplicado** — `reach` com `period=week` e `period=days_28`,
também em `time_series`. Devolve, para cada dia, o alcance único dos 7 e dos 28 dias
anteriores. **É a única forma correta de ter alcance de período**, porque somar o diário
infla 1,33× (medido). Equivale ao `reach_week`/`reach_days_28` do conector oficial.

**2. Agregado sem recorte** — `metric_type=total_value`, **um request por dia**. Saem com
`scope: total`, `breakdown_key` nulo.

| Métrica | Significado |
|---|---|
| `views` | visualizações |
| `likes` | gostos |
| `comments` | comentários |
| `shares` | partilhas |
| `saves` | guardados — **na conta é `saves`; na mídia é `saved`** |
| `replies` | respostas |
| `reposts` | republicações |
| `total_interactions` | interações com conteúdos |
| `accounts_engaged` | contas que interagiram |
| `profile_views` | visitas ao perfil |
| `website_clicks` | toques na ligação do site |
| `profile_links_taps` | toques nas ligações do perfil |
| `follows_and_unfollows` | seguimentos e cancelamentos — **nulo sem breakdown** |

**3. Recorte por formato** — `breakdown=media_product_type`, um request por dia. É daqui que
sai o impulsionado.

`views`, `reach`, `likes`, `comments`, `saves`, `shares`, `total_interactions`
→ dimensões `POST`, `REEL`, `CAROUSEL_CONTAINER`, `STORY`, `AD`.

**4. Outros recortes** — um request por dia cada.

| Métrica | `breakdown_key` | Dimensões |
|---|---|---|
| `reach`, `follows_and_unfollows` | `follow_type` | `FOLLOWER`, `NON_FOLLOWER` |
| `profile_links_taps` | `contact_button_type` | `CALL`, `EMAIL`, `TEXT`, `DIRECTION`, `BIO_LINK_CLICKED`, `OTHER` |

**Custo:** 4 sub-requests por dia, todos em batch de 50. Medido: 28 dias em 2 chamadas de
batch, 6,4s, 1.070 linhas. Backfill de 730 dias ≈ 30 chamadas, ~3 min.

**Janela própria:** `account_lookback_days`, default **7**. A Meta revisa o número por até
48h; reler 28 dias de conta todo dia é desperdício. É parâmetro separado do
`lookback_window_days` da mídia.

Rejeitadas: `impressions`, `email_contacts`, `phone_call_clicks`, `text_message_clicks`,
`get_directions_clicks`, `profile_visits`, `threads_views`, `online_followers` (incompatível
com `period=day`), `content_views` (responde vazio).

## `account_demographics` — demografia dos seguidores

`GET /{ig-user-id}/insights?metric=follower_demographics&period=lifetime` · Full refresh +
Append · chave `business_account_id + metric + breakdown_key + breakdown_value + snapshot_date`

`timeframe` obrigatório (default `last_30_days`), quatro recortes, um request cada:

| `breakdown_key` | Devolve |
|---|---|
| `age` | 7 faixas (`13-17` … `65+`) |
| `gender` | `F`, `M`, `U` |
| `country` | 45 maiores |
| `city` | 45 maiores |

`engaged_audience_demographics` e `reached_audience_demographics` ficam fora da v1 (P4): só
respondem em `this_week`/`this_month`.

## `media` — a publicação, append-once

`GET /{ig-user-id}/media` · Incremental + Append · cursor `timestamp` · chave `media_id`

| Campo | Observação |
|---|---|
| `media_id`, `ig_id`, `shortcode`, `permalink` | identificação |
| `timestamp` | cursor |
| `caption` | texto |
| `media_type` | `IMAGE`, `VIDEO`, `CAROUSEL_ALBUM` |
| `media_product_type` | `FEED`, `REELS` — **define a lista de métricas** |
| `is_comment_enabled` | relevante para o monitoramento: mídia sem comentário habilitado não é "sem reclamação" |
| `username`, `owner_id` | autoria |
| `alt_text` | acessibilidade |

Fora: `media_url`, `thumbnail_url` (expiram) e `children` (um request por item de carrossel).

## `media_metrics` — contadores, por snapshot

Mesma listagem de `/media`, campos de contagem · Incremental + Append ·
chave `media_id + snapshot_date`

| Campo | Observação |
|---|---|
| `like_count` | **é o orgânico**: igual ao insight `likes` em 80 de 81 mídias medidas |
| `comments_count` | usado também como gate da stream `comment` |

**Esta stream não respeita `insights_max_age_days`, só `lookback_window_days`.** É de
propósito: os contadores sobrevivem à retenção de 2 anos que destrói `reach` e `views`
(invariante 5). É o único jeito de acompanhar mídia antiga sem gravar lixo. Custo zero de
request: os campos vêm na mesma listagem de `media`.

## `media_insights` — métrica × mídia × snapshot

`GET /{media-id}/insights` em batch de 50 · Incremental + Append ·
chave `media_id + metric + scope + breakdown_key + breakdown_value + snapshot_date`

Colunas: `media_id`, `metric`, `scope`, `breakdown_key`, `breakdown_value`, `value_numeric`,
`value_json`, `is_derived`, `title`.

Só mídia dentro de `lookback_window_days` **e** com menos de `insights_max_age_days`.

**`FEED`** — imagem e carrossel (15 métricas):

| Métrica | Escopo que produz |
|---|---|
| `reach` | organic |
| `views` | organic (+ facebook, paid, total pelos pares) |
| `likes` | organic (+ paid, total) |
| `comments` | organic (+ paid, total) |
| `saved` | organic |
| `shares` | organic |
| `follows` | organic |
| `profile_visits` | organic |
| `profile_activity` | organic, breakdown `action_type` |
| `total_interactions` | organic |
| `reposts` | organic |
| `facebook_views` | facebook |
| `total_views`, `total_likes`, `total_comments` | total |

**`REELS`** (16 métricas): as mesmas, **menos** `follows`, `profile_visits` e
`profile_activity` (a API recusa), **mais**:

| Métrica | Observação |
|---|---|
| `ig_reels_avg_watch_time` | média de visualização, em ms |
| `ig_reels_video_view_total_time` | tempo total, em ms, com repetições |
| `reels_skip_rate` | **percentual**, não contagem (ex.: 55,9) |
| `crossposted_views` | Instagram + Facebook agregados |

`crossposted_views` e `facebook_views` falham com subcode `2207086` em mídia não cruzada —
ver a regra 3 do modelo de escopo.

## `story` — inventário do que está no ar

`GET /{ig-user-id}/stories` · Full refresh + Append · chave `story_id + snapshot_date`

Campos: `story_id`, `timestamp`, `media_type`, `media_product_type`, `permalink`,
`shortcode`, `caption`.

Diferente do Facebook, a story **tem `id` próprio** e é endereçável direto.

## `story_insights` — métrica × story × snapshot

`GET /{story-id}/insights` em batch de 50 · Full refresh + Append ·
chave `story_id + metric + breakdown_key + breakdown_value + snapshot_date`

13 métricas — o oficial pede 7:

| Métrica | Novo em relação ao oficial |
|---|---|
| `reach`, `views`, `replies`, `shares`, `follows`, `profile_visits`, `total_interactions` | — |
| `navigation` | **sim** — breakdown `story_navigation_action_type`: `SWIPE_FORWARD`, `TAP_BACK`, `TAP_EXIT`, `TAP_FORWARD` |
| `link_clicks` | **sim** — cliques em ligação |
| `profile_activity` | **sim** — breakdown `action_type` |
| `reposts` | **sim** |
| `facebook_views` | **sim** |
| `total_views` | **sim** |

`navigation` e `link_clicks` são as que dizem se a story funcionou, e hoje não existem no
lake.

**Story com menos de 5 espectadores devolve `(#10)`**: a story é gravada e a métrica fica
ausente. Não é falha de sync.

## `comment` — comentário e resposta

`GET /{media-id}/comments` · Incremental + Append · cursor `timestamp` por mídia ·
chave `comment_id`

| Campo | Observação |
|---|---|
| `comment_id`, `media_id`, `parent_id` | thread; `parent_id` preenchido é resposta |
| `timestamp`, `text` | conteúdo |
| `like_count`, `hidden` | estado |
| `username`, `from_id` | **autor** — exige `instagram_manage_comments` **e** a task `MODERATE` na página ligada (P1b). Hoje só a Texaco preenche |

Respostas vêm por field expansion (`replies{...}`), nunca por request por comentário.
Gate: a mídia só é paginada se `comments_count` mudou desde o último state.

`from_id` é o **IGSID**, não o id público do perfil — não resolve como URL.

---

## Resumo do que muda para a equipe de monitoramento

| Pergunta que hoje não tem resposta no lake | Como passa a ter |
|---|---|
| "Quanto desse alcance foi pago?" | `scope` em `account_insights` e `media_insights` |
| "Quantas visualizações teve esta foto?" | `views` passou a responder em imagem e carrossel |
| "Esta story funcionou?" | `navigation` e `link_clicks` em `story_insights` |
| "O que estão comentando?" | stream `comment`, que não existe no oficial |
| "Qual era o alcance real deste post de 2023?" | não tem — e o conector para de fingir que tem (invariante 5) |
