# Documentação de referência e achados de API

Fontes usadas para desenhar o conector, com o que cada uma resolve e o extrato que importa.
A seção final registra o que foi **verificado contra a API real** com o token do projeto —
tem precedência sobre a documentação quando divergirem.

## Airbyte — desenvolvimento de conector

| Link | Resolve |
|---|---|
| https://docs.airbyte.com/platform/connector-development/cdk-python | Python CDK: `AbstractSource`, `HttpStream`, `IncrementalMixin`, slicing, state, backoff |
| https://docs.airbyte.com/platform/connector-development/ | visão geral, scaffolding do gerador, `metadata.yaml`, Dockerfile |
| https://docs.airbyte.com/platform/connector-development/testing-connectors/connector-acceptance-tests-reference | testes de aceitação (CAT): `spec`, `check`, `discover`, `read`, incremental |
| https://docs.airbyte.com/integrations/sources/instagram | conector base que estamos adaptando: streams, spec e limitações |
| https://docs.airbyte.com/integrations/destinations/s3 | destino S3/MinIO: formatos, path format, **modos de sync suportados** |
| https://docs.airbyte.com/platform/using-airbyte/getting-started/add-a-connector | registrar conector customizado por imagem Docker na instância |

O conector oficial, como a própria doc dele diz: *"Incremental sync modes are only available
for the User Insights stream."* Streams e modos:

| Stream | Modo | Cursor |
|---|---|---|
| `users` | full refresh | — |
| `user_insights` | incremental | `date` |
| `user_lifetime_insights` | full refresh | — |
| `media` | full refresh | — |
| `media_insights` | full refresh | — |
| `stories` | full refresh | — |
| `story_insights` | full refresh | — |

Pontos que condicionam o desenho:

- **`media` e `media_insights` são full refresh.** É daí que vem o custo: as 2.722 mídias
  das 5 contas são relidas a cada sync, com um request de insights por mídia.
- **O destino S3 não oferece `Incremental + Dedup`.** Os modos são full refresh
  overwrite/append e incremental append. Logo o conector entrega append com `snapshot_date`
  e a deduplicação é do bronze — não é contorno, é o desenho certo para data lake.
- A lista de métricas por tipo de mídia é **fixa no manifest declarativo** do conector
  oficial — é o que impede ajustar métricas e janela sem fork.

## Instagram Platform API — conta

https://developers.facebook.com/docs/instagram-platform/instagram-graph-api/reference/ig-user

- Campos do node: `id`, `username`, `name`, `biography`, `website`, `followers_count`,
  `follows_count`, `media_count`, `is_published`, `has_profile_pic`, `profile_picture_url`,
  `legacy_instagram_user_id`.
- Edges: `media`, `stories`, `live_media`, `insights`, `tags`, `business_discovery`,
  `content_publishing_limit`.
- O id da conta sai de `/me/accounts?fields=instagram_business_account{...}`, que também
  entrega o `page_id` da página ligada — a única junção disponível entre as duas redes.

## Instagram Platform API — insights de conta

https://developers.facebook.com/docs/instagram-platform/api-reference/instagram-user/insights

- `GET /{ig-user-id}/insights?metric=...&period=day&metric_type=...&since=...&until=...`
- **Dois mundos:** `metric_type=time_series` (série por dia, só `reach` e `follower_count`) e
  `metric_type=total_value` (um agregado do intervalo, as outras 13). Ver
  [metrics.md](metrics.md).
- `breakdown` aceita `media_product_type`, `follow_type`, `contact_button_type`, `age`,
  `gender`, `country`, `city`, conforme a métrica. O resultado vem em
  `total_value.breakdowns[].results[]` com `dimension_values` e `value`.
- Demografia é `period=lifetime` + `timeframe` (nunca `since`/`until`), exige 100 seguidores
  e devolve no máximo os 45 maiores em `country` e `city`.
- **Janela máxima de 30 dias por request.**
- Atraso de até 48h na consolidação dos números.

## Instagram Platform API — mídia

https://developers.facebook.com/docs/instagram-platform/instagram-graph-api/reference/ig-user/media

- `GET /{ig-user-id}/media` devolve **no máximo as 10 mil mídias mais recentes** e **não
  inclui stories** (edge própria).
- **Aceita `since` / `until`** sobre `timestamp` — é o que viabiliza `start_date` e janela,
  e é exatamente o que o conector oficial não usa.
- Campos de interesse: `id`, `ig_id`, `caption`, `permalink`, `shortcode`, `timestamp`,
  `media_type`, `media_product_type`, `like_count`, `comments_count`,
  `is_comment_enabled`, `username`, `owner`, `alt_text`.
- Campos **deliberadamente não usados**: `media_url`, `thumbnail_url` (URL de CDN que
  expira) e `children` (um request extra por item de carrossel).
- `media_type` é `IMAGE`, `VIDEO` ou `CAROUSEL_ALBUM`; `media_product_type` é `FEED`,
  `REELS` ou `STORY`. **A lista de métricas depende do segundo**, não do primeiro.

## Instagram Platform API — insights de mídia

https://developers.facebook.com/docs/instagram-platform/reference/instagram-media/insights

- `GET /{media-id}/insights?metric=...` — todas `lifetime`, logo a mesma mídia devolve valor
  diferente em dias diferentes.
- Métrica não suportada para o tipo devolve `#100` com mensagem nomeando a métrica e o tipo.
- `crossposted_views` e `facebook_views` em reel não cruzado devolvem `code -1 / Fatal /
  subcode 2207086 / is_transient: false` — **HTTP 400, não transitório**.
- `total_likes`, `total_comments` e `total_views` incluem anúncio e Facebook, e só existem
  com token de Facebook Login.

## Instagram Platform API — stories

https://developers.facebook.com/docs/instagram-platform/instagram-graph-api/reference/ig-user/stories

- `GET /{ig-user-id}/stories` só devolve story **no ar**: sem coleta horária, não há dado.
- Diferente do Facebook, a story **tem `id` próprio** e é endereçável: `/{story-id}/insights`
  responde direto. No `facebook_organic` foi preciso usar `media_id` porque o edge não
  devolvia `id`.
- Campos: `id`, `timestamp`, `media_type`, `media_product_type`, `permalink`, `shortcode`,
  `caption`, `thumbnail_url`.
- Métrica de story com menos de 5 espectadores: `(#10) Not enough viewers for the media to
  show insights`.

## Instagram Platform API — comentários

https://developers.facebook.com/docs/instagram-platform/instagram-graph-api/reference/ig-comment

- `GET /{media-id}/comments`
- Campos do node: `id`, `timestamp`, `text`, `like_count`, `hidden`, `parent_id`, `username`,
  `from{id,username}`, `user`, `media{id,media_product_type}`, `replies`.
- **`username`, `from` e `user` exigem `instagram_manage_comments`.** Sem a permissão a API
  aceita o `fields`, responde 200 e omite os campos — não erra.
- `from.id` é o **IGSID**, não o id público do perfil.
- Respostas por field expansion aninhada (`replies{...}`), com `limit` explícito nos dois
  níveis.
- Não existe `filter=stream` como no Facebook: a listagem traz o 1º nível e as respostas vêm
  aninhadas ou pela edge `/{comment-id}/replies`.

## Plataforma

| Link | Resolve |
|---|---|
| https://developers.facebook.com/docs/graph-api/guides/versioning | ciclo de vida das versões; a versão é parâmetro do conector |
| https://developers.facebook.com/docs/graph-api/changelog | o que morreu em cada versão — consultar antes de fixar métricas |
| https://developers.facebook.com/docs/graph-api/overview/rate-limiting | limites e headers `x-app-usage`, `x-business-use-case-usage` |
| https://developers.facebook.com/docs/graph-api/batch-requests | até 50 sub-requests por POST — usado para insights de mídia |
| https://developers.facebook.com/docs/graph-api/results | paginação por cursor e por tempo (`since`/`until`) |
| https://developers.facebook.com/docs/graph-api/field-expansion | expansão de campos e aninhamento com `limit` |
| https://developers.facebook.com/docs/permissions | permissões e Advanced Access |
| https://developers.facebook.com/docs/graph-api/reference/debug_token | conferir escopos e *granular scopes* de um token |

### Permissões necessárias

| Permissão | Para que | Temos? |
|---|---|---|
| `instagram_basic` | node da conta, mídia, stories | sim |
| `instagram_manage_insights` | insights de conta, de mídia e de story | sim |
| `pages_show_list` | descobrir a conta IG pela página ligada | sim |
| `pages_read_engagement` | leitura da página ligada | sim |
| `business_management` | contas via Business Manager com token de System User | sim |
| `instagram_manage_comments` | autor do comentário (`username`, `from`, `user`) e a edge `/tags` | sim, desde 14/10 |

**O conteúdo e o autor passam por portas diferentes:**

| O que | Exige | Observado |
|---|---|---|
| `text`, `timestamp`, `like_count`, `hidden`, `parent_id`, `replies` | `instagram_basic` + `pages_read_user_content` | responde nas 5 contas, com qualquer nível de task na página ligada |
| `username`, `from{id,username}`, `user` | `instagram_manage_comments` **e** task `MODERATE` na página ligada | só responde onde as duas existem |
| `GET /{ig-user-id}/tags` | idem | idem |

Sem a permissão de App, a API aceita o `fields`, responde 200 e omite os campos. **Com a
permissão e sem a task, o comportamento é o mesmo** — campo vazio, sem erro, o que torna o
diagnóstico difícil: nada na resposta diz qual das duas portas está fechada.

`instagram_manage_comments` é concedida pelo diálogo de geração de token de usuário de
sistema no Business Manager, sem Análise do App, por ser ativo do próprio negócio. A task
`MODERATE` é concedida ativo a ativo, na página ligada à conta — o mesmo trabalho que o
`facebook_organic` exige.

### Rate limit

O Instagram usa limite por caso de uso de negócio, informado em
`x-business-use-case-usage` com `type: instagram` — e com **uma entrada por conta IG e uma
pelo app**, não uma por página como no Facebook. Cada entrada traz `call_count`,
`total_cputime`, `total_time` e `estimated_time_to_regain_access`. O conector lê esse header
e faz backoff; `sleep` fixo é proibido.

Requests por sync e por conta ≈ 1 (conta) + fatias de 30 dias da série nativa + um por dia
de `total_value` + 4 a 12 de demografia + páginas de `/media` + mídias na janela ÷ 50 (batch)
+ stories + mídias com comentário novo. Na ordem de **40 requests por conta por sync**.

## Achados verificados contra a API

Medições e validações datadas ficam em
[plans/connector-v1.md](plans/connector-v1.md); aqui fica só o que é comportamento
permanente da plataforma:

- **Todas as edges respondem com o token de System User**, sem derivar token por objeto:
  `/{ig-user-id}`, `/media`, `/stories`, `/insights`, `/{media-id}/insights`,
  `/{story-id}/insights`, `/{media-id}/comments`. Não existe aqui o passo que o
  `facebook_organic` precisa (erro 190/210 nas edges de página).
- `/media` aceita `since`/`until` e devolve em ordem decrescente de `timestamp`, o que
  permite interromper a paginação ao sair da janela.
- **`metric_type=total_value` devolve agregado do intervalo, não série.** Pedir 25 dias
  devolve um número só; pedir `since=D, until=D+1` devolve o dia. As 13 métricas cabem num
  request.
- **13 métricas de conta em um request; 50 mídias em um batch.** Os dois lotes funcionam e
  são a diferença entre janela barata e janela caótica.
- `code -1 / "Fatal"` chega como **HTTP 400** com `is_transient: false` e subcode
  específico. O status HTTP não distingue erro permanente de transitório — `is_transient` e o
  subcode distinguem.
- **`media_count` do node não bate com o total de `/media`**: Texaco 1.737 contra 1.479,
  Loterias 631 contra 552, Binder 602 contra 542. A diferença não é o teto de 10 mil. Não
  investigado — o mesmo descompasso já estava registrado no plano do `binder_etl`.
- `/{ig-user-id}/tags` (mídia de terceiro que marcou a conta) exige
  `instagram_manage_comments` **e** a task `MODERATE`: responde na Texaco e devolve
  `(#10) Application does not have permission for this action` nas contas que só têm
  `ANALYZE`. `mentions` não existe como field.
- A retenção de ~2 anos **destrói insight e preserva contador** — ver a medição por faixa de
  idade no plano e a nota em [metrics.md](metrics.md).
