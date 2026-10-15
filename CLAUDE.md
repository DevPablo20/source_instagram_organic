# source_instagram_organic

Conector Airbyte (Python CDK) para conteúdo **orgânico** de contas profissionais do
Instagram: conta, insights de conta, demografia de seguidores, mídia (feed, carrossel e
reels), insights de mídia, stories, insights de story e **comentários**. Substitui o
`source-instagram` oficial no `binder_etl` — o oficial é aposentado, não mantido em paralelo.

**Por que existir:** o conector oficial só tem incremental em `user_insights`. `media` e
`media_insights` são full refresh: cada sync relê as 2.722 mídias das 5 contas e pede
insights mídia por mídia, então a duração cresce com a idade da conta. Pior, metade dessas
mídias tem mais de dois anos, e aí a API **não devolve o número histórico — devolve um
número degradado** (ver invariante 5), que a releitura diária grava como se fosse o valor
atual. Este conector corta a janela (`start_date`, `lookback_window_days`), congela o que
já estabilizou, para de reler o que a plataforma já destruiu (`insights_max_age_days`),
data cada leitura (`snapshot_date`) e adiciona comentários — que o oficial não cobre.

Além do custo, o oficial deixa de fora quase tudo que a API passou a oferecer: `views` de
imagem e carrossel, o recorte orgânico × pago por `media_product_type`, as 13 métricas
`total_value` de conta (`views`, `likes`, `saves`, `shares`, `profile_views`,
`total_interactions`, …) e 6 das 13 métricas de story. Ver [docs/metrics.md](docs/metrics.md).

**Uso fim:** ETL para a equipe de monitoramento acompanhar contas, publicações e
comentários dos clientes, substituindo ferramentas externas de social listening
(Brandwatch, Zing).

## Estado

As 9 streams estão implementadas e lidas contra a API real; a imagem Docker está construída
localmente e ainda **não foi publicada**. Decisões (fechadas e pendentes), status, medições
e diário em [docs/plans/connector-v1.md](docs/plans/connector-v1.md). Referências externas e
achados de API em [docs/docs.md](docs/docs.md).

## Invariantes

Contradizer uma delas é bug, não escolha de implementação.

1. **Custo de sync é constante, não função da idade da conta.** Nenhum stream varre o
   histórico inteiro a cada execução. Toda stream derivada de mídia opera dentro de uma
   janela de dias configurável.
2. **Métrica acumulada é série, não estado.** Todo valor de insight e todo contador
   (`like_count`, `comments_count`) é gravado com `snapshot_date` e `extracted_at`. A linha
   nunca é sobrescrita; o "valor atual" é o último snapshot.
3. **Uma conexão, uma conta.** `instagram_business_account_id` é campo único da spec. Ainda
   assim todo registro de toda stream carrega `business_account_id` e `page_id`, para o lake
   não depender do caminho do arquivo e para a junção com o `facebook_organic` existir.
4. **Feed, carrossel e reel são a mesma stream; story é outra.** `media` cobre os três
   porque vêm da mesma edge com o mesmo shape — o que muda é a lista de métricas, por
   `media_product_type`. Story vem de edge própria e vive 24h: nunca se mistura com mídia.
5. **Insight de mídia com mais de ~2 anos é lixo, não histórico.** Medido: o `reach` de um
   post de 2024-09-30 era 1.666 e hoje a API devolve **81**; acima de ~900 dias `views`
   devolve **0**. Os contadores (`like_count`, `likes`) sobrevivem; `reach` e `views` são
   destruídos. Logo mídia mais velha que `insights_max_age_days` **não é consultada** —
   reler grava o número degradado como se fosse o atual e corrompe a série do bronze, onde
   o último snapshot vence.
6. **Impulsionado é uma coluna, não uma fórmula.** Toda stream de insight tem `scope`
   (`organic`, `facebook`, `paid`, `total`). O conector normaliza o nome da métrica e põe a
   origem no escopo: `views`/`facebook_views`/`total_views` viram a métrica `views` em três
   escopos, e o `paid` é o que fecha a conta. A equipe de monitoramento filtra; não
   recalcula. Detalhe em [docs/streams.md](docs/streams.md).
7. **Conta não precisa de token derivado.** O token de System User responde em todas as
   edges testadas (`/{ig-user-id}`, `/media`, `/stories`, `/insights`, `/{media}/comments`).
   Não existe aqui o passo de derivar token por objeto que o `facebook_organic` precisa.
8. **Nenhum token em registro de saída, log, mensagem de erro ou traceback.**

## Streams da v1

| Stream | Endpoint | Modo | Cursor | Chave natural | Grão |
|---|---|---|---|---|---|
| `account` | `/{ig-user-id}` | Full refresh + Append | — | `business_account_id + snapshot_date` | foto diária da conta |
| `account_insights` | `/{ig-user-id}/insights` | Incremental + Append | `date` | `business_account_id + metric + period + scope + breakdown_key + breakdown_value + date` | métrica × recorte × dia |
| `account_demographics` | `/{ig-user-id}/insights` (`*_demographics`) | Full refresh + Append | — | `business_account_id + metric + breakdown_key + breakdown_value + snapshot_date` | demografia × recorte × snapshot |
| `media` | `/{ig-user-id}/media` | Incremental + Append | `timestamp` | `media_id` | mídia publicada (feed, carrossel, reel) |
| `media_metrics` | `/{ig-user-id}/media` (campos de contagem) | Incremental + Append | `timestamp` da mídia | `media_id + snapshot_date` | contadores × mídia × snapshot |
| `media_insights` | `/{media-id}/insights` | Incremental + Append | `timestamp` da mídia | `media_id + metric + scope + breakdown_key + breakdown_value + snapshot_date` | métrica × mídia × snapshot |
| `story` | `/{ig-user-id}/stories` | Full refresh + Append | — | `story_id + snapshot_date` | story enquanto está no ar |
| `story_insights` | `/{story-id}/insights` | Full refresh + Append | — | `story_id + metric + breakdown_key + breakdown_value + snapshot_date` | métrica × story × snapshot |
| `comment` | `/{media-id}/comments` | Incremental + Append | `timestamp` do comentário, por mídia | `comment_id` | comentário e resposta |

`media_metrics` existe separado de `media` porque contador muda e legenda não: assim `media`
fica append-once e a série de `like_count`/`comments_count` não duplica a legenda a cada
snapshot. Mesma razão da separação `post`/`post_metrics` no `facebook_organic`.

**Não existe stream de reel.** No Facebook o reel é edge própria e precisou de
`reel_insights`; aqui o reel é mídia com `media_product_type = REELS` na mesma edge, e as
métricas próprias dele (`ig_reels_avg_watch_time`, `reels_skip_rate`, …) entram em
`media_insights` pela lista daquele tipo.

`comment` entrega texto e thread em todas as contas. O **autor** depende da task `MODERATE`
na página ligada, que hoje só a Texaco tem (P1b): nas outras 4 contas `username` e `from_id`
nascem vazios, sem erro.

## Regras duras

- **Destino é S3/MinIO, que não suporta dedup.** Toda conexão é **Append**; `Overwrite`
  apaga os arquivos anteriores do stream. A deduplicação acontece no bronze do `binder_etl`,
  pela chave natural da tabela acima + `snapshot_date`.
- **Só `reach` e `follower_count` têm série diária nativa.** As outras 13 métricas de conta
  existem apenas como `metric_type=total_value`, que devolve **um agregado do intervalo
  pedido, não um valor por dia**. A série diária delas se obtém pedindo **um dia por
  request** (`since=D`, `until=D+1`), com as 13 métricas num só request. Somar `total_value`
  de intervalo largo e chamar de "dia" é erro de leitura, não otimização.
- **Janela máxima de 30 dias por request de insights de conta.** `There cannot be more than
  30 days (2592000 s) between since and until`. O intervalo entre `start_date` e hoje é
  fatiado de 30 em 30 dias — no Facebook o limite era 90.
- **`impressions` morreu e não volta.** Rejeitada em conta (`#100`, com a mensagem
  enumerando as válidas) e em mídia (`Starting from version v22.0 and above, the impressions
  metric is no longer supported`). O substituto é `views`. Diferente do Facebook, aqui a
  mensagem de erro **enumera** as métricas válidas para a família legada — mas só para ela.
- **`code -1 "Fatal"` não é erro transitório.** Vem como **HTTP 400** com
  `error_subcode: 2207086` e `is_transient: false`, e significa "esta métrica não existe
  para este objeto" — acontece com `crossposted_views` e `facebook_views` em reel que não
  foi publicado cruzado no Facebook. Tratar como 500 transitório e repetir 8 vezes por mídia
  queima o sync. **O sinal de retry é `is_transient` e o subcode, nunca o status HTTP.**
- **`reach` nunca é somado — nem entre recortes, nem entre dias.** É conta única. Somar os
  recortes de um dia dá 187.850 contra 186.668 reais; somar 28 dias dá 1.597.665 contra
  1.199.023 reais, uma inflação de **1,33×**. Alcance de período vem de `period=week` e
  `period=days_28`, que a API entrega já deduplicado. Todas as outras métricas são aditivas
  por dia — `views` fecha em 1,0000×.
- **Valor de métrica pode ser negativo.** `saves` é líquido de "desguardar" e a dimensão
  `AD` já devolveu **−1**. Schema aceita negativo; ETL não trata como erro.
- **Subcode `2207086` significa zero, não ausente.** `facebook_views` só falha assim quando
  a mídia não foi publicação cruzada — ou seja, não houve reprodução no Facebook. Grava-se
  `0` com `is_derived`, o que mantém a decomposição de `paid` fechada. Tratar como nulo
  quebraria o cálculo em 30 das 81 mídias medidas. **Acontece em `FEED` também, não só em
  reel.**
- **`saves` é conta; `saved` é mídia.** Os dois nomes existem, em níveis diferentes, e cada
  um é rejeitado no nível do outro. Não unificar.
- **Métrica inválida derruba o request inteiro.** A lista é pedida em lote; na recusa, o
  conector isola métrica por métrica uma vez só e descarta a morta com aviso — mesmo
  mecanismo do `facebook_organic`, agora também para o subcode 2207086.
- **`(#200) Permissions error` é por métrica, não por objeto.** Medido numa story em que
  `total_views` falha e as outras 12 métricas respondem. Tratar como erro do objeto perde as
  12; o código entra no mesmo isolamento das outras recusas.
- **Objeto que falha no lote é refeito sozinho.** O lote devolve o erro da requisição
  inteira, sem dizer qual métrica o causou; refazer fora do lote aciona o isolamento. Vale
  para mídia e, principalmente, para story — que expira em 24h e não se relê amanhã.
- **Métrica não validada contra a API não entra na lista.** A lista vem de descoberta contra
  conta real, não da documentação: a doc da Meta lista nomes que a API rejeita e omite
  recortes que ela aceita.
- **Lista de métricas é por `media_product_type`.** `FEED` (imagem e carrossel) aceita
  `follows`, `profile_visits` e `profile_activity`, que `REELS` recusa; `REELS` aceita a
  família `ig_reels_*`, `reels_skip_rate` e `crossposted_views`, que `FEED` recusa. Pedir a
  lista errada derruba o request.
- **Batch de 50 sub-requests é obrigatório em insights de mídia.** Medido: 50 mídias em 5,5s
  por batch contra 36s uma a uma — 6,5×. Sem batch a janela deixa de ser barata.
- **Story só existe por 24h.** `story` e `story_insights` dependem de sync horário; em sync
  diário a story simplesmente não existe quando o conector olha. Nunca incremental por
  cursor — é foto do que está no ar.
- **Insight de story com menos de 5 espectadores é erro, não zero.** `(#10) Not enough
  viewers for the media to show insights`: a story existe e a métrica não. O conector grava
  a story e deixa a métrica ausente, sem tratar como falha.
- **`start_date` é configurável e truncado, nunca fixo.** Pedido anterior ao limite da
  plataforma é elevado ao limite, com log explícito. O conector nunca falha por `start_date`
  antigo.
- **Janelas separadas, parâmetros separados.** `lookback_window_days` (28) governa
  reextração de mídia; `account_lookback_days` (7) governa a reextração de insights de
  conta, que a Meta fecha em 48h; `insights_max_age_days` (700) é o teto de idade acima do
  qual a métrica já não é confiável (invariante 5); `comment_window_days` (90) governa até
  que idade de mídia os comentários continuam sendo vigiados. Não compartilham valor.
- **`insights_max_age_days` vale para `media_insights`, não para `media_metrics`.** Os
  contadores (`like_count`, `comments_count`) sobrevivem à retenção que destrói `reach` e
  `views`, e vêm de graça na listagem de `/media`. É o único jeito de acompanhar mídia
  antiga sem gravar lixo.
- **Antes de paginar comentários, confira o total.** `comments_count` vem na própria listagem
  de `/media` e custa zero request extra; se o total não mudou desde o último state, a mídia
  é pulada.
- **Respostas de comentário vêm por field expansion** (`replies{...}` aninhado), não por
  request por comentário. Request por comentário reintroduz o problema que o conector existe
  para resolver.
- **Identidade de autor de comentário exige `instagram_manage_comments`.** Sem ela a API
  aceita `fields=username,from,user`, responde 200 e **omite os campos silenciosamente** —
  não erra. Confundir campo omitido com "a API não devolve autor" foi o que aconteceu no
  Facebook, onde de fato não devolve; aqui devolve, com a permissão certa.
- **Conteúdo de comentário e autor de comentário têm portas diferentes.** O texto responde
  nas 5 contas, inclusive nas que só têm `ANALYZE` na página ligada. O **autor** exige as
  duas coisas ao mesmo tempo: a permissão de App `instagram_manage_comments` **e** a task
  `MODERATE` (*Atividade da comunidade*) na página ligada. Verificado: com a permissão
  concedida, a Texaco (`MODERATE`) devolve `username` e `from`; Loterias, CAIXA e Binder
  (só `ANALYZE`) devolvem os dois campos vazios, sem erro. A task vale página a página — é
  o mesmo trabalho da P7b do `facebook_organic`.
- **Versão da Graph API é parâmetro**, com default único em um só lugar do código.
- **Rate limit é tratado por backoff lendo `x-business-use-case-usage`**, onde o Instagram
  aparece com `type: instagram` (não `pages`) e **uma entrada por conta e uma pelo app**.
  `sleep` fixo é proibido.
- **Mudar `start_date`, as janelas ou a lista de métricas exige Clear data** dos streams
  afetados no Airbyte.

## Configuração (spec do source)

| Campo | Tipo | Default | Papel |
|---|---|---|---|
| `access_token` | secret | — | token de System User do Business Manager |
| `instagram_business_account_id` | string | — | uma conta por conexão |
| `start_date` | date | — | corte histórico duro |
| `lookback_window_days` | int | 28 | reextração de insights e contadores de mídia |
| `account_lookback_days` | int | 7 | reextração de insights de conta (a Meta fecha o dia em 48h) |
| `insights_max_age_days` | int | 700 | teto de idade: acima disso a métrica já foi degradada pela plataforma |
| `comment_window_days` | int | 90 | idade máxima de mídia vigiada por comentário |
| `include_replies` | bool | true | respostas de comentário na mesma stream |
| `account_metrics` / `media_metrics_feed` / `media_metrics_reels` / `story_metrics` | array | listas validadas | métricas por stream e por tipo de mídia |
| `demographics_timeframe` | string | `last_30_days` | janela da demografia de seguidores |
| `api_version` | string | `v26.0` | versão da Graph API |

## Dado pessoal

Aqui a questão **não** se resolve sozinha como no Facebook. A API do Instagram devolve o
autor do comentário (`from.id` IGSID e `username`) quando o App tem
`instagram_manage_comments` — é dado pessoal de terceiro, entrando por decisão, não por
acidente. A decisão de pedir a permissão está tomada (D11); a responsabilidade pela LGPD é
do Pablo, como no Facebook.

Mesmo sem a permissão, **o texto do comentário já identifica pessoas**: as respostas da
própria marca começam com `@handle` do interlocutor (`"@watson26076 Exemplo de garra…"`), de
modo que a identidade do autor é parcialmente recuperável do texto. Não há mascaramento no
conector — é inerente à finalidade de monitoramento.

Fora, por decisão: **URL de mídia de CDN** (`media_url`, `thumbnail_url`,
`profile_picture_url`) — expira em dias e rehospedar imagem de terceiro não serve a nenhum
requisito. Entram `permalink` e `shortcode`; a equipe abre o link quando precisa.

## Integração com binder_etl

O ETL vive em `/var/www/binder_app/binder_etl` (medallion Spark + Delta no MinIO) e **já tem
um pipeline `instagram_organic` completo** (bronze, silver e gold compartilhada com o
Facebook) consumindo o conector oficial. Trocar o conector é mudança lá, não aqui.

- Destino: bucket `raw`, path format **`airbyte/instagram_organic/{account_id}/{stream}`**.
  Hoje o ETL lê `airbyte/instagram_organic/{stream}`, sem pasta por conta, porque a conexão
  oficial é única para as 5 contas. Com uma conexão por conta (D2) o path ganha a pasta, por
  simetria com o `facebook_organic` — e isso **exige mudar o `RAW_PREFIX` do
  `src/transformers/instagram_organic/tables.py`**. Ver a pendência P3 no plano: manter
  `{stream}` nas 5 conexões também funciona e não mexe no ETL.
- Conexões por conta: **diária** (todos os streams menos story, 01:00 America/Sao_Paulo,
  antes do DAG `instagram_organic_daily`), **horária de stories** (`story`,
  `story_insights`) e **horária de crise** (`media`, `media_metrics`, `comment`), ligada sob
  demanda. Com 5 contas isso são 10 conexões permanentes, contra 2 hoje — custo conhecido da
  D2.
- A gold `gold/organic/` é compartilhada com o Facebook e já tem `metrics_scope`
  (`organic` × `total`), que é exatamente onde entram os pares `views`/`total_views` da
  invariante 6.
- As convenções de bronze/silver/gold, nomes e testes seguem o `CLAUDE.md` do `binder_etl`.
  Mudança de schema aqui é mudança lá.

## Documentação

| Arquivo | Quando ler |
|---|---|
| [docs/docs.md](docs/docs.md) | links e extratos: Instagram Platform API, CDK, destino S3, limites, permissões e achados de API |
| [docs/airbyte.md](docs/airbyte.md) | **guia de uso do source**: o que cada stream significa para o negócio, métricas e como configurar — é o `documentationUrl` do conector |
| [docs/streams.md](docs/streams.md) | **contrato de cada stream**: campos, lista de métricas, modelo de escopo (orgânico × impulsionado) e chaves |
| [docs/metrics.md](docs/metrics.md) | quais métricas respondem hoje, por tipo de mídia, e o que cada uma significa |
| [docs/plans/connector-v1.md](docs/plans/connector-v1.md) | decisões fechadas e pendentes, próxima ação, medições datadas |

## Onde cada informação mora

| Tipo | Onde |
|---|---|
| Regra que vale sempre | `CLAUDE.md` |
| Como e por que funciona; referência externa | `docs/*.md` — no presente, sem data |
| O que falta, status, decisão em aberto, medição datada | `docs/plans/connector-v1.md` |
