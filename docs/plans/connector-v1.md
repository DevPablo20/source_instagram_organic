# connector-v1 — escopo e andamento

Rastreabilidade do projeto: o que já está decidido, o que ainda está aberto e quem decide.
Regra deste arquivo: decisão fechada vira regra no `CLAUDE.md` e aparece aqui com data;
medição fica aqui, nunca em `docs/*.md`.

**Status:** passos 1 a 7 entregues (código completo, 9 streams lidas contra a API real,
imagem Docker construída e validada localmente). Falta publicar a imagem e registrar o
conector no Airbyte — nada disso foi feito ainda.
**Próxima ação:** passo 7d — criar o source da Texaco e a conexão diária, depois o primeiro
sync real contra o MinIO. Em paralelo e por conta do Pablo, a P1b.

## Ambientes

| | Desenvolvimento | Produção |
|---|---|---|
| Airbyte | `localhost:8080` | — |
| MinIO | `localhost:9000` (console 9001) | `192.168.10.80:9000` (console 9001) |
| Uso | onde este conector roda e é validado | só leitura, para comparação |

**Validação final da v1:** rodar a Texaco e comparar com o baseline do conector oficial
(2.722 mídias relidas por sync, `instagram_organic` com 1,5 GiB em 317 objetos no lake).

## Contas no escopo

Um token de System User alcança 5 contas profissionais, todas pela página ligada:

| Conta | `business_account_id` | Página | Seguidores | `media_count` | `/media` |
|---|---|---|---|---|---|
| `texacolubrificantes` | 17841408010423009 | Texaco Lubrificantes | 140.859 | 1.737 | 1.479 |
| `loteriascaixaoficial` | 17841407270613285 | Loterias CAIXA | 583.949 | 631 | 552 |
| `caixa` | 17841401357494265 | Se liga na Caixa | 2.686.048 | 109 | 103 |
| `binder` | 17841404676020635 | Binder | 9.196 | 602 | 542 |
| `caixainveste` | 17841442952681475 | CAIXA Investe | 43.037 | 49 | 46 |

As páginas CAIXA, CAIXA Investe (101259635957075) e a segunda CAIXA Investe não têm conta de
Instagram ligada. Total de mídia histórica: **2.722**.

## Decisões fechadas

| # | Decisão | Data | Nota |
|---|---|---|---|
| D1 | Fork próprio em Python CDK clássico, não low-code nem patch do oficial | 2026-10-14 | mesma razão do `facebook_organic`: lookback, snapshot, sub-stream e batch não cabem bem no declarativo |
| D2 | **Uma conexão por conta** (`instagram_business_account_id` único na spec) | 2026-10-14 | decisão do Pablo: simetria com o `facebook_organic`, isola falha e rate limit por conta. Custo: 10 conexões permanentes contra 2 hoje, e o path do lake muda (ver P3) |
| D3 | Janela fixa + snapshot para métricas que mudam | 2026-10-14 | `lookback_window_days` default 28; mídia fora da janela congela |
| D4 | `snapshot_date` + `extracted_at` em toda stream de insight e contador | 2026-10-14 | resolve o rastreio que falta hoje |
| D5 | `start_date` configurável no source, truncado ao limite da plataforma | 2026-10-14 | nunca falhar por data antiga; log do truncamento |
| D6 | Sync em Incremental + Append, dedup no bronze do `binder_etl` | 2026-10-14 | destino S3 não suporta dedup |
| D7 | Estrutura nova de tabelas; conector oficial é aposentado | 2026-10-14 | sem compatibilidade de schema com o antigo |
| D8 | `media` (append-once) separado de `media_metrics` (série) | 2026-10-14 | não reescrever a legenda da mídia a cada snapshot |
| D9 | **Sem stream de reel.** Reel é mídia com `media_product_type = REELS` | 2026-10-14 | diferente do Facebook, onde o reel é edge própria e exigiu `reel_insights` |
| D10 | Métrica entra na lista só depois de responder na API, **por tipo de mídia** | 2026-10-14 | a lista é por `media_product_type`; pedir a lista errada derruba o request |
| D11 | **A stream `comment` é requisito da v1 e espera `instagram_manage_comments`** | 2026-10-14 | decisão do Pablo: comentário sem autor não serve ao monitoramento. As outras streams não ficam bloqueadas |
| D12 | **Não derivar token por objeto** | 2026-10-14 | verificado: o token de System User responde em todas as edges. O passo que o `facebook_organic` precisa (D16 lá) não existe aqui |
| D13 | **`insights_max_age_days` default 700**: mídia mais velha não é consultada | 2026-10-14 | medido: `reach` de 1.666 virou 81 em dois anos; acima de ~900 dias `views` é 0. Reler grava lixo como valor atual |
| D14 | **Pares orgânico × total entram os dois** (`views`/`total_views`, `likes`/`total_likes`, `comments`/`total_comments`) | 2026-10-14 | revisada pela D23: viram a mesma métrica em escopos diferentes |
| D15 | **Breakdown `media_product_type` entra em `account_insights`** | 2026-10-14 | é o que separa `AD` do orgânico na conta — 48% do `views` da Texaco é pago e hoje entra no lake como orgânico |
| D16 | Série diária das 13 métricas `total_value` por **um request por dia** | 2026-10-14 | verificado: `total_value` é agregado do intervalo, não série. As 13 cabem num request |
| D17 | Batch de 50 sub-requests em insights de mídia | 2026-10-14 | medido: 6,5× mais rápido que uma a uma |
| D18 | `story` e `story_insights` em streams separadas, em conexão horária | 2026-10-14 | story some em 24h; 13 métricas em formato longo não cabem como colunas de `story` |
| D19 | `api_version` default **`v26.0`** | 2026-10-14 | mesma versão do `facebook_organic`; toda a descoberta foi feita nela |
| D20 | URL de mídia de CDN fica fora (`media_url`, `thumbnail_url`, `profile_picture_url`) | 2026-10-14 | expira em dias; `permalink` e `shortcode` bastam |
| D21 | Integração no `binder_etl` sai do escopo deste repositório | 2026-10-14 | feita direto lá, como no `facebook_organic` |
| D22 | Imagem `devpablo20/source-instagram-organic` no Docker Hub, repositório público | 2026-10-14 | mesma convenção do `facebook_organic`, versionada `x.y.z` + `latest` |
| D23 | **Coluna `scope` (`organic`/`facebook`/`paid`/`total`) em toda stream de insight**, com o nome da métrica normalizado | 2026-10-15 | impulsionado vira filtro, não fórmula. `paid` de `views` é exato; de `likes`/`comments` absorve o Facebook. Linha calculada leva `is_derived` |
| D24 | **Path do lake `airbyte/instagram_organic/{account_id}/{stream}`**, conectores separados por conta | 2026-10-15 | decisão do Pablo: simetria com o `facebook_organic`. O ETL será refeito do lado do `binder_etl` — fecha a P3 |
| D25 | **`engaged_audience_demographics` e `reached_audience_demographics` ficam fora** | 2026-10-15 | decisão do Pablo: entram depois, se forem necessárias — fecha a P4 |
| D26 | **`account_lookback_days` default 7**, separado do `lookback_window_days` da mídia | 2026-10-15 | a Meta fecha o dia em 48h; reler 28 dias de conta por dia é desperdício |
| D27 | **`media_metrics` não respeita `insights_max_age_days`** | 2026-10-15 | contador sobrevive à retenção que destrói `reach` e `views`, e vem de graça na listagem de `/media` |
| D28 | **Subcode `2207086` vira `facebook_views = 0`**, não nulo | 2026-10-15 | é "não foi publicação cruzada", logo não houve reprodução no Facebook. Nulo quebraria o `paid` em 30 das 81 mídias |
| D29 | **`reach` também em `period=week` e `days_28`**, e coluna `period` na chave de `account_insights` | 2026-10-15 | somar alcance diário infla 1,33×; alcance de período só existe se a API entregar deduplicado |
| D30 | **A documentação do conector vive nas descrições da spec**, não no painel do Airbyte | 2026-10-15 | o painel não renderiza URL externa em conector custom (ver medição); a descrição de campo renderiza e fica onde a decisão é tomada |
| D31 | `documentationUrl` aponta para `docs/airbyte.md` no GitHub; versão sobe para **0.1.1** | 2026-10-15 | mudança de spec é mudança de conector |

## Pendências

### Elementares — fechar antes de implementar

| # | Pendência | Quem decide |
|---|---|---|
| P1b | **Ligar a task *Atividade da comunidade* (`MODERATE`) nas páginas Loterias CAIXA, Se liga na Caixa, Binder e CAIXA Investe.** A permissão de App já existe, mas o autor do comentário e a edge `/tags` exigem também a task na página ligada. Sem ela a stream `comment` nasce sem autor nessas 4 contas — a mesma pendência que o `facebook_organic` tem em P7b, e que se resolve na mesma visita ao Business Manager | Pablo |

A P1b degrada **um campo** da stream `comment` em 4 das 5 contas; não bloqueia nenhum passo.
Os passos 1 a 6 podem começar.

- ~~P1~~ `instagram_manage_comments`: **concedida em 14/10**, sem Análise do App. Aparece em
  `scopes` e em `granular_scopes`; autor e `/tags` respondem onde a task existe.
- ~~P2~~ Análise do App para Acesso Avançado: não foi necessária.
- ~~P3~~ path do lake: conectores separados por conta, `{account_id}/{stream}` (D24).
- ~~P4~~ demografia de engajados e alcançados: fora da v1 (D25).

### Opcionais — não bloqueiam, dá para ligar depois sem retrabalho

| # | Pendência | Nota |
|---|---|---|
| P5 | Suíte de testes de aceitação (CAT) | qualidade de entrega, não requisito funcional |
| P6 | Conexão horária de crise por conta | configuração no Airbyte, não código |
| P7 | Investigar o descompasso `media_count` × `/media` (Texaco 1.737 × 1.479) | já estava aberto no plano do `binder_etl`; não afeta o desenho |
| P9 | **Semântica de `follows_and_unfollows` com `breakdown=follow_type`.** As dimensões são `FOLLOWER` (2.685) e `NON_FOLLOWER` (1.315), o mesmo enum que em `reach` significa "alcançou seguidor × não-seguidor". Aqui a leitura provável é "ganhou × perdeu", mas não está confirmada | conferir contra o Business Suite quando o silver for montado; a extração não muda |
| P8 | **Stream `mention`** a partir de `/{ig-user-id}/tags` — mídia de terceiro que marcou a conta | **já responde** na Texaco: 10 mídias, com `username` do autor, legenda, permalink e contadores. É o caso de uso de social listening que motiva o projeto (substituir Brandwatch/Zing): hoje nenhum conector traz conteúdo de terceiro sobre o cliente. Avaliar para v1.1 |

### Descartadas — fora da v1, com motivo

| Item | Motivo |
|---|---|
| Stream de reel separada | reel é mídia na mesma edge (D9) |
| Derivação de token por conta | o token de System User responde em tudo (D12) |
| Menção de terceiro (`/tags`) | **destravada em 14/10**: responde na Texaco com 10 mídias de terceiro. Fica fora da v1 por escopo, não por bloqueio — ver P8 |
| `children` de carrossel | um request extra por item, sem requisito que o justifique |
| URL de mídia de CDN | expira (D20) |
| `impressions` em qualquer nível | morta na v22.0+; o substituto é `views` |
| Insight de mídia com mais de ~2 anos | a API devolve número degradado (D13) |
| Conexão multi-conta | decisão do Pablo (D2) |

## Passos

Cada passo termina com algo verificável. Nenhum passo começa antes do anterior fechar.

| # | Passo | Entrega | Verificação |
|---|---|---|---|
| 1 ✅ | Esqueleto | `setup.py`, `main.py`, `source_instagram_organic/`, `spec.json`, `Dockerfile`, `metadata.yaml` | `spec` devolve os 14 campos, 3 obrigatórios |
| 2 ✅ | Cliente e `check` | `api.py` com paginação, batch de 50, backoff por `x-business-use-case-usage`, redação de token, classificação de erro por `is_transient`/subcode e `InsightsResult` | `check` passa na Texaco, avisa sobre o autor no Binder e falha com mensagem legível em conta inexistente e em token inválido |
| 3 ✅ | Streams de conta | `account`, `account_insights` (série nativa fatiada de 30 em 30 dias + `total_value` dia a dia com breakdowns), `account_demographics` | `read` devolve série diária desde `start_date` sem erro de janela, com `AD` separado do orgânico |
| 4 ✅ | Streams de mídia | `media` (append-once), `media_metrics`, `media_insights` com lista por `media_product_type`, lookback, `insights_max_age_days` e batch | sync da Texaco cobre a janela de 28 dias e não varre as 1.479 mídias |
| 5 ✅ | Stories | `story` + `story_insights` com as 13 métricas e tolerância ao `(#10)` de menos de 5 espectadores | story capturada antes de expirar, com `navigation` e `link_clicks` |
| 6 ✅ | Comentários (autor depende de P1b) | `comment` com janela própria, gate por `comments_count`, respostas por field expansion e autor | comentários da Texaco chegam com thread montada e `username` preenchido |
| 7 ✅ | Empacotamento | imagem `devpablo20/source-instagram-organic:0.1.0` e `:latest` construídas (627 MB) | `spec` e `check` rodam dentro do container |
| 7b ✅ | Publicação da imagem | `devpablo20/source-instagram-organic:0.1.1` e `:latest` no Docker Hub, repositório público | digest `sha256:6f4f7eec…`, 140 MB comprimidos, visível anonimamente |
| 7c ✅ | Registro no Airbyte | conector custom `Instagram Organic`, tag 0.1.1, registrado no Airbyte de desenvolvimento | aceito; *open docs* abre o guia |
| 7d | Source e conexão | source da Texaco e conexão diária | sync real gravando em `raw/airbyte/instagram_organic/{account_id}/{stream}` |
| 8 | Comparação com o baseline (depende de 7d) | medir duração e volume contra os números de 14/10 | ganho demonstrado, não presumido |

A integração no `binder_etl` (troca do conector no bronze/silver/gold do `instagram_organic`
e aposentadoria do oficial) é feita **no repositório do `binder_etl`**, não aqui.

## Medições

**2026-10-14 — token e acessos.** Token do `.env` do `source_facebook_organic` é SYSTEM_USER
do App `BI_Binder` (`2082474985856996`), sem expiração. Escopos: `ads_management`,
`ads_read`, `business_management`, `instagram_basic`, `instagram_manage_insights`,
`pages_read_engagement`, `pages_read_user_content`, `pages_show_list`, `public_profile`,
`read_insights`. **Granular scopes** trazem `instagram_basic` e `instagram_manage_insights`
(e não trazem `read_insights` nem `public_profile`). **`instagram_manage_comments` não está
em nenhuma das duas listas** — é a P1.

**2026-10-14 — edges com token de System User.** Respondem sem derivar token:
`/{ig-user-id}` (todos os campos), `/{ig-user-id}/media` (com `since`/`until`),
`/{ig-user-id}/stories`, `/{ig-user-id}/insights`, `/{media-id}/insights`,
`/{story-id}/insights`, `/{media-id}/comments`. Falham: `/{ig-user-id}/tags` (`#10`),
`content_publishing_limit` (`#10`), `mentions` (field inexistente). `live_media` responde
vazio.

**2026-10-14 — volume por conta e por janela.**

| Conta | 7d | 28d | 90d | 365d | histórico | páginas de 100 | tempo do scan |
|---|---|---|---|---|---|---|---|
| texaco | 1 | 19 | 79 | 302 | 1.479 | 16 | 10,6s |
| loterias | 5 | 21 | 82 | 261 | 552 | 7 | 4,8s |
| caixa | 6 | 18 | 81 | 103 | 103 | 3 | 1,8s |
| binder | 0 | 1 | 7 | 58 | 542 | 7 | 4,5s |
| caixainveste | 3 | 18 | 36 | 46 | 46 | 2 | 1,2s |
| **total** | **15** | **77** | **285** | **770** | **2.722** | **35** | **22,9s** |

**Conclusão:** a janela de 28 dias cobre **77 mídias** contra as **2.722** que o conector
oficial relê a cada sync — **35× menos**. Dessas 2.722, mais de mil já passaram da retenção
de 2 anos e devolvem número degradado.

**2026-10-14 — custo de insights de mídia.** 77 mídias da janela, métricas em lote por tipo:
786 linhas de métrica em **55,4s** (0,72s por mídia), uma a uma. Extrapolando para as 2.722
do histórico: **32,6 min por sync**, que é a ordem de grandeza do que o oficial faz hoje.
Com batch de 50 sub-requests: **50 mídias em 5,5s** (0,11s por mídia) — **6,5× mais rápido**,
o que põe a janela de 28 dias em ~8s e o histórico inteiro em ~5 min.

**2026-10-14 — a retenção de 2 anos destrói insight (fecha a D1 do plano do
`binder_etl`).** O plano do ETL deixou aberto: *"o post `18153850864324974` (Texaco,
30/09/2024 09:12, alcance 1.666) — se o alcance cair, o corte existe"*. **Caiu: hoje a API
devolve `reach` 81** para esse post, com `views` 2.980 e `like_count` 140 intactos. Medição
por faixa de idade, imagem de feed da Texaco, uma por faixa de 90 dias:

| Idade (dias) | `reach` | `views` | `likes` | `like_count` |
|---|---|---|---|---|
| 77 | 1.646 | 2.624 | 56 | 56 |
| 94 | 4.068 | 6.128 | 234 | 234 |
| 183 | 3.412 | 5.427 | 108 | 108 |
| 273 | 2.925 | 4.550 | 75 | 75 |
| 362 | 1.404 | 27.991 | 40 | 40 |
| 451 | 2.780 | 5.166 | 173 | 173 |
| 553 | 1.520 | 2.302 | 57 | 57 |
| 632 | 2.437 | 3.204 | 77 | 77 |
| **720** | **239** | 2.446 | 105 | 105 |
| **811** | **31** | 1.808 | 73 | 73 |
| 901 | 21 | **0** | 55 | 55 |
| 992 | 21 | **0** | 83 | 83 |
| 1.082 | 11 | **0** | 266 | 266 |
| 1.175 | 7 | **0** | 100 | 100 |
| 1.262 | 3 | **0** | 57 | 57 |
| 1.366 | 7 | **0** | 19 | 19 |
| 1.446 | 2 | **0** | 29 | 29 |
| 1.530 | 3 | **0** | 45 | 45 |

O corte está entre 632 e 720 dias para `reach` e por volta de 900 para `views`. Os
contadores (`likes`, `like_count`) sobrevivem íntegros em toda a faixa. Daí a D13: **não é
só economia, é integridade** — com o bronze tomando o último snapshot como valor atual, uma
releitura tardia substitui 1.666 por 81. O post do Binder de 23/03/2017 confirma o fim da
curva: `reach` 6, `views` 0, `like_count` 43.

**2026-10-14 — insights de conta: dois mundos.** `metric_type=time_series` responde só para
`reach` e `follower_count` (25 valores em 25 dias pedidos). As outras 13 recusam
`time_series` com `The following metric (x) is incompatible with the metric type
(time_series)` e, em `total_value`, devolvem **um agregado do intervalo**: 25 dias da Texaco
vieram como `views` 2.243.411, `likes` 60.050, `reach` 723.278 — um número só. Pedindo
`since=D, until=D+1`, as 13 métricas num request devolvem o dia:

| Dia | `views` | `likes` | `comments` | `shares` | `saves` | `total_interactions` | `accounts_engaged` | `profile_views` |
|---|---|---|---|---|---|---|---|---|
| 10/10 | 38.151 | 78 | 2 | 1 | 3 | 86 | 83 | 85 |
| 11/10 | 45.137 | 95 | 1 | 4 | 1 | 105 | 77 | 95 |
| 12/10 | 48.114 | 236 | 6 | 3 | 7 | 262 | 232 | 103 |

Janela máxima confirmada: 30 dias responde, 31 falha com `There cannot be more than 30 days
(2592000 s) between since and until`.

**2026-10-14 — o pago que entra como orgânico.** `breakdown=media_product_type` na Texaco,
25 dias: `views` POST 550, STORY 58.132, **AD 1.086.168** (de 2.243.411 totais — 48%);
`likes` POST 1.213, REEL 47.667, **AD 10.085**; `reach` POST 202, REEL 8.675, STORY 6.351.
`reach` com `breakdown=follow_type`: FOLLOWER 8.956, NON_FOLLOWER 715.946.
`follows_and_unfollows` só tem valor com breakdown: FOLLOWER 2.685, NON_FOLLOWER 1.315.
**Hoje o lake recebe o total e o chama de orgânico.**

**2026-10-14 — demografia.** `follower_demographics` responde nos 6 timeframes; `age` 7
faixas, `gender` 3 valores (F 25.054, M 97.954 na Texaco), `country` e `city` limitados a 45
linhas. `engaged_audience_demographics` e `reached_audience_demographics` só respondem em
`this_week` e `this_month`: os timeframes longos foram desligados (*"no longer supported"*).

**2026-10-14 — métricas de mídia por tipo.** 35 nomes testados em cada tipo. `FEED` (imagem
e carrossel): 15 responderam. `REELS`: 16 responderam. Lista com significado em
[docs/metrics.md](../metrics.md). **`views` responde em imagem e carrossel** — o plano do
`binder_etl` registra isso como fora de escopo porque o conector oficial só pede `views` para
reels. Tipos na janela de 28 dias: 32 `VIDEO/REELS`, 23 `CAROUSEL_ALBUM/FEED`, 22
`IMAGE/FEED` — nenhum `VIDEO/FEED`.

**2026-10-14 — a armadilha do reel não cruzado.** 26 das 77 mídias da janela falharam com
`code -1 / "Fatal"`. Isolado: a causa é `crossposted_views` e `facebook_views` em reel que
não foi publicado cruzado no Facebook. Determinístico (5 tentativas, 5 falhas) e vem como
**HTTP 400** com `error_subcode: 2207086`, `is_transient: false` e `error_user_msg`
explicando o caso. **Se o cliente tratar como 500 transitório, repete 8 vezes por mídia.** O
sinal correto de retry é `is_transient`, não o status.

**2026-10-14 — stories.** Ativas no momento da medição: Texaco 4, Loterias 7, CAIXA 1,
Binder 0, CAIXA Investe 0. A story **tem `id` próprio** e `/{story-id}/insights` responde
direto — diferente do Facebook, onde o edge não devolve `id` e foi preciso usar `media_id`.
13 métricas respondem; o conector oficial pede 7. `navigation` devolve 46 como escalar e o
detalhe por `story_navigation_action_type` dentro de `total_value.breakdowns`.

**2026-10-14 — comentários.** `/{media-id}/comments` devolve conteúdo com o token atual:
`id`, `timestamp`, `text`, `like_count`, `hidden`, `parent_id`, `media`, `replies`. O reel
`18201754369329104` (`comments_count` 13) devolveu 6 comentários de 1º nível com as
respostas aninhadas por field expansion. **`username`, `from` e `user` são aceitos no
`fields`, a resposta vem 200 e os campos simplesmente não aparecem** — não é erro, é omissão
silenciosa por falta de `instagram_manage_comments` (P1). Observado também que o texto das
respostas da marca começa com o `@handle` do interlocutor
(`"@watson26076 Exemplo de garra e inspiração!"`), de modo que a identidade é parcialmente
recuperável do texto mesmo sem a permissão.

**2026-10-14 — conteúdo de comentário não tem gate de task.** Testadas as 5 contas: a Texaco
tem todas as tasks na página ligada, as outras 4 só `ANALYZE`. **Todas devolvem conteúdo de
comentário.** Em 365 dias: Texaco 93 mídias com comentário (1.487 no total), Binder 46 (610),
Loterias 11 (182), CAIXA 3 (323), CAIXA Investe 0 — esta última com 8 mídias de comentário
desabilitado.

**2026-10-14 (tarde) — token regerado com `instagram_manage_comments`; o autor tem um
segundo gate.** Token novo (211 chars) idêntico nos três lugares: `.env` do
`source_facebook_organic`, `secrets/config.json` e `.env` deste repositório. SYSTEM_USER,
sem expiração, 11 escopos. **`instagram_manage_comments` aparece em `scopes` e em
`granular_scopes`** — Acesso Padrão bastou, sem Análise do App.

Mas a permissão sozinha não entrega o autor. Correlacionando com as tasks da página ligada:

| Conta | Tasks da página | Autor do comentário | `/tags` |
|---|---|---|---|
| `texacolubrificantes` | `MANAGE, CREATE_CONTENT, MODERATE, …` | **5/5 com `username` e `from`** | **10 mídias de terceiro** |
| `loteriascaixaoficial` | `ANALYZE` | 0/5 | `#10` |
| `caixa` | `ANALYZE` | 0/4 | `#10` |
| `binder` | `ANALYZE` | 0/5 | `#10` |
| `caixainveste` | `ANALYZE` | sem mídia com comentário | `#10` |

Exemplo na Texaco: `username='watson26076'`, `from={"id": "943694301748737", "username":
"watson26076"}` — o `id` é o IGSID. `user` vem nulo (só preenche se o próprio App criou o
comentário).

**Correção do que estava escrito antes:** a P7b do `facebook_organic` **existe** no
Instagram também, só que restrita ao autor — o texto do comentário responde sem a task. E o
modo de falhar é pior que no Facebook: com a permissão concedida e sem a task, a API
responde 200 e devolve o campo vazio, sem erro e sem dizer qual das duas portas está
fechada. Virou a P1b.

**2026-10-14 (tarde) — regressão com o token novo.** `check` do `source_facebook_organic`:
`Check succeeded`, com derivação do Page Access Token e conexão à Texaco. Derivação de token
e `page_insights` + `published_posts` verificados também em CAIXA e Loterias CAIXA. No
Instagram, as 9 verificações passam: node, `/media`, `/stories`, insights `time_series`,
insights `total_value`, demografia, insights de mídia, comentários com autor e `/tags`.
`read_insights` continua fora dos `granular_scopes` e continua funcionando, como antes.

**2026-10-14 — rate limit.** `x-business-use-case-usage` devolve `type: instagram` com
**duas entradas**: uma pelo id do negócio/app e uma pelo `business_account_id`. Campos
`call_count`, `total_cputime`, `total_time`, `estimated_time_to_regain_access`. Após ~700
requests de descoberta, uso permaneceu em 1%. Volume não é restrição; o gargalo do oficial é
o varrimento histórico.

**2026-10-14 — baseline do lake (192.168.10.80:9000), herdado da medição do
`facebook_organic`.** `raw/airbyte` tem 3,8 GiB em 899 objetos, dos quais
`instagram_organic` **1,5 GiB em 317 objetos** — o segundo maior prefixo, atrás do
`facebook_organic` (1,7 GiB). Como no Facebook, boa parte é padding de parquet
(`block_size_mb: 128`), não conteúdo.

**2026-10-15 — a decomposição do impulsionado fecha.** Conta (Texaco, 7 dias): a soma dos
breakdowns de `media_product_type` bate **exatamente** com a métrica sem breakdown em
`views`, `likes`, `comments`, `saves`, `shares` e `total_interactions`. **`reach` não bate**:
soma 187.850 contra total 186.668 (+1.182), porque é conta única e a mesma pessoa aparece em
mais de uma dimensão. Dimensões observadas: `POST`, `REEL`, `CAROUSEL_CONTAINER`, `STORY`,
`AD` — a quarta não tinha aparecido na medição de 14/10. `AD` foi 58,0% das `views` e 98,2%
do `reach` em 7 dias. `saves` da dimensão `AD` devolveu **−1**: métrica líquida pode ser
negativa.

**2026-10-15 — decomposição por mídia, 81 mídias da janela de 28 dias.** Fórmula
`total_views − views − facebook_views`:

| Resultado | Mídias |
|---|---|
| resíduo 0 — orgânico puro | 37 |
| resíduo > 0 — impulsionado | 14 |
| resíduo < 0 — incoerente | **0** |
| sem `facebook_views` (subcode 2207086) | 30 |

Nas 30 sem `facebook_views`, tratar como zero dá resíduo ≥ 0 em todas (27 zero, 3 positivo),
o que sustenta a D28. **16 das 30 são `FEED`**, não só reel — a documentação diz que o erro
é de reel, e não é.

Maiores impulsionamentos medidos: CAIXA com 38.456 `views` orgânicas e **5.703.283 pagas**
(99,3% do total de 5.742.225); Texaco com 5.252 orgânicas e 121.359 pagas. **Hoje o lake não
enxerga nada disso** — o oficial só pede `views` de reel.

`like_count` da listagem é igual ao insight `likes` em **80 de 81** mídias: o contador é
orgânico. `total_likes − likes` é sempre ≥ 0 (zero casos incoerentes), mas não há
`facebook_likes`, então o derivado absorve o Facebook junto com o anúncio.

**2026-10-15 — custo do dia a dia com batch.** 28 dias × 2 formas de pedido = 56
sub-requests em **2 chamadas de batch, 6,4s, 1.070 linhas** de métrica. Backfill de 730 dias
≈ 30 chamadas, ~3 min. O pedido por dia, que parecia caro, é barato em lote.

`reach` do mesmo dia pelos dois caminhos devolve o mesmo número (39.511 em 11/10 por
`time_series` e por `total_value`): a série nativa serve para o total e o pedido por dia
serve para o recorte, sem conflito.

**2026-10-15 — `reach` não é aditivo entre dias; o resto é.** Texaco, 28 dias: somar o
`reach` diário dá **1.597.665** contra **1.199.023** que a API devolve para o período —
inflação de **1,33×**. `views` somado nos mesmos 28 dias dá 3.164.141 contra 3.164.141,
razão **1,0000**: aditivo. `period=week` e `period=days_28` respondem em `time_series` e
devolvem o alcance móvel já deduplicado (é o `reach_week`/`reach_days_28` do oficial); em
`total_value` são rejeitados. Daí a D29.

**2026-10-15 — armadilha de nome confirmada:** `saves` só existe na conta e `saved` só na
mídia; cada um é rejeitado com `#100` no nível do outro.

**2026-10-15 — passos 1 e 2 entregues e verificados.** Ambiente: `.venv` com Python 3.11.15
do pyenv (o `python3` do host é 3.12 e está sem `ensurepip`), `airbyte-cdk>=7,<8`.

`spec` devolve 14 campos, 3 obrigatórios. `check` na Texaco: conecta, resolve a página
ligada (280663778456254) e confirma insights.

Caminhos de falha exercitados:

| Cenário | Resultado |
|---|---|
| Binder (só `ANALYZE`) | `SUCCEEDED` **com aviso** nomeando a task `MODERATE` que falta |
| Conta inexistente | `FAILED`, mensagem da própria API |
| `start_date` `01/01/2025` | `FAILED` no validador da spec, antes do código |
| Token inválido | `FAILED` com code 190, **sem o token na mensagem** |

Cliente exercitado contra a API real: `paginate` com `since/until` (20 mídias em 0,4s),
`linked_page_id` com cache, `batch` de 50 (20 sub-requests em 2,0s, alinhamento preservado),
isolamento por métrica nos dois motivos e `redact()` na URL.

**Achado novo:** `crossposted_views` em mídia `FEED` não cruzada devolve **`#100`** ("does
not support ... for this media product type"), e não o subcode 2207086 que `facebook_views`
devolve na mesma mídia. São dois motivos diferentes para a mesma situação, e o
`InsightsResult` os separa (`invalid_metric` × `metric_unavailable`) — só o segundo vira
zero. A mensagem do `#100` ainda enumera as 23 métricas válidas de mídia, o que confirma que
as listas de `docs/streams.md` estão completas.

**2026-10-15 — leitura real das 9 streams (Texaco, `start_date` 2026-01-01).**

| Stream | 1ª execução (backfill) | 2ª execução (com state) |
|---|---|---|
| `account` | 1 | 1 |
| `account_insights` | 13.399 | **347** |
| `account_demographics` | 100 | 100 |
| `media` | 227 | **0** |
| `media_metrics` | 227 | 20 |
| `media_insights` | 4.198 | **371** |
| `story` / `story_insights` | 6 / 106 | 6 / 107 |
| `comment` | 3.335 | **0** |
| **total** | **21.599 registros, 6min37s** | **952 registros, 32,6s** |

O backfill cobriu 288 dias de conta (1.152 sub-requests em lote, 2min19s), 227 mídias
(1min21s) e 3.335 comentários (2min57s). Na segunda execução a janela de conta caiu para 8
dias, a de mídia para 20 mídias e **75 mídias foram puladas pelo gate de `comments_count`**.
`media` devolveu zero por ser append-once.

**2026-10-15 — conservação verificada nos 288 dias.** A soma dos recortes de
`media_product_type` bate com a métrica sem recorte em **todos os dias** para `views`,
`likes`, `comments`, `saves`, `shares` e `total_interactions`. **`reach` difere nos 288
dias**, como manda a natureza de conta única. Somar 28 dias de `reach` diário dá 1.399.918
contra 1.055.049 do `period=days_28`: **1,33× de inflação**, o mesmo fator medido antes.

**2026-10-15 — a decomposição do impulsionado fecha em 227 de 227 mídias.** Zero casos
incoerentes no backfill inteiro. **171 das 227 mídias foram impulsionadas.** As maiores:

| Mídia | Orgânico | Pago | % pago |
|---|---|---|---|
| 18021485279685211 | 2.579 | 2.024.379 | 99,9% |
| 18133302895627878 | 1.527 | 1.844.219 | 99,9% |
| 18101881942989455 | 1.445 | 1.676.037 | 99,9% |
| 17881177719498387 | 6.273 | 1.237.496 | 99,5% |

Hoje o lake recebe só o orgânico dessas mídias, e sem dizer que é orgânico.

**2026-10-15 — comentários no backfill.** 3.335 comentários de 209 mídias, de 05/01 a
15/10, **todos com autor**, 1.233 respostas ligadas por `parent_id`, 1.232 autores
distintos, nenhum `comment_id` duplicado, nenhum oculto.

**2026-10-15 — `(#200) Permissions error` é por métrica, não por objeto.** A story
`18111462047062814` falhava inteira no lote. Isolando: `total_views` devolve `#200` e as
outras 12 métricas respondem (`reach` 240, `views` 297, `navigation` 278). Antes da
correção a story saía com **zero** linhas; depois, com 16. Como story não se relê, o
conector passou a refazer sozinho qualquer objeto que falhe no lote — o lote não diz qual
métrica causou o erro, e só o pedido isolado descobre.

**2026-10-15 — o painel de documentação do Airbyte não aceita URL externa.** Para conector
custom adicionado por imagem Docker, a documentação oficial do Airbyte diz que o campo
*Connector documentation URL* serve para **linkar**, não para renderizar: *"This
documentation will be linked in your connector setting's page"*
([operator guide](https://docs.airbyte.com/platform/operator-guides/using-custom-connectors)).
O painel lateral resolve o `documentationUrl` para um markdown **local** do próprio
Airbyte — comportamento descrito na
[issue #9015](https://github.com/airbytehq/airbyte/issues/9015) —, e para um conector custom
esse arquivo não existe, daí o painel vazio. Não foi possível confirmar no código atual: o
`airbyte-webapp` não está mais no monorepo.

**Conclusão prática:** o lugar que de fato documenta o conector dentro do Airbyte são as
**descrições dos campos da spec**, que aparecem no formulário de configuração. Foram
reescritas (155 a 645 caracteres cada, todas preenchidas), e `docs/airbyte.md` passou a ser
o destino do `documentationUrl` e o texto para a descrição do repositório no Docker Hub.

**2026-10-15 — imagem Docker publicada.** `devpablo20/source-instagram-organic:0.1.1` e
`:latest` no Docker Hub, repositório **público**, digest `sha256:6f4f7eec…`, 627 MB
descomprimidos / 140 MB comprimidos. `spec` e `check` rodam dentro do container, com o
`secrets/` montado somente-leitura. A maioria das camadas foi reaproveitada do
`source-facebook-organic`, que usa a mesma base.

**O painel de documentação confirmou o esperado:** com o conector registrado, o campo vira
um botão *open docs* que abre a URL; nada é renderizado dentro do Airbyte. O
`full_description` do Docker Hub recebeu o [airbyte.md](../airbyte.md) (13.910 caracteres),
então o link leva ao guia completo. A descrição curta do repositório segue vazia.

## Diário

**2026-10-15 (noite)** — Implementados os passos 3 a 7: as 9 streams, os schemas e a
imagem. O backfill da Texaco rodou inteiro e a segunda execução caiu de 21.599 para 952
registros, que é o conector fazendo exatamente o que foi desenhado para fazer. As regras de
conservação foram verificadas nos 288 dias, e a decomposição do impulsionado fechou em todas
as 227 mídias. Dois defeitos apareceram só na execução real e viraram regra: `(#200)` é por
métrica, e objeto que falha no lote precisa ser refeito sozinho — sem isso uma story saía
com zero linhas e não haveria amanhã para corrigir.

**2026-10-15 (tarde)** — Implementados os passos 1 e 2. O cliente nasceu diferente do
`facebook_organic` em dois pontos que a descoberta tinha exigido: não há token a derivar, e
o retry decide por `is_transient` em vez do status HTTP. O `InsightsResult` foi a forma de
não perder, no caminho, a informação de *por que* uma métrica não voltou — é o que permite
cumprir a D28 lá na frente.

**2026-10-15** — Pablo fechou P3 (conectores separados por conta, ETL refeito depois do lado
do `binder_etl`) e P4 (demografia de engajados e alcançados fora por ora), e pediu o desenho
do recorte orgânico × impulsionado e a lista de métricas por stream. Medida a decomposição
nos dois níveis: na conta ela é nativa pelo breakdown `AD`, na mídia é derivada e fecha sem
um caso incoerente em 81 mídias. Disso saiu a D23 — `scope` como coluna, não como fórmula —
e com ela a invariante 6 foi reescrita. Três achados colaterais viraram regra: `reach` não
soma, valor pode ser negativo e o subcode 2207086 significa zero. Criado
[docs/streams.md](../streams.md) com o contrato de cada stream.

**2026-10-14** — Escopo levantado e fechado. Lidos o `source_facebook_organic` inteiro
(`CLAUDE.md`, `docs/`, plano e código) e o pipeline `instagram_organic` já existente no
`binder_etl`, para herdar convenções e não repetir o que lá já está resolvido. Descoberta
executada contra a API real com o token do projeto: edges, volume por janela, shape de
comentário, stories e listas de métricas por tipo de mídia.

Cinco achados mudam o desenho em relação ao conector do Facebook:

1. **Não há token a derivar** (D12) — o inverso da D16 do Facebook.
2. **Insights de conta vivem em dois mundos** e só `reach`/`follower_count` têm série
   nativa; as outras 13 exigem um request por dia (D16).
3. **A retenção de 2 anos destrói `reach` e `views`** e preserva contadores, o que
   transforma a janela de economia em requisito de integridade (D13) e fecha a D1 que estava
   aberta no plano do `binder_etl`.
4. **`code -1 "Fatal"` é HTTP 400 permanente**, não 500 transitório — e o campo que
   distingue é `is_transient`, que o cliente do Facebook não lê.
5. **48% do `views` da conta da Texaco é `AD`** e hoje entra no lake como orgânico; o
   breakdown que separa existe e não era pedido (D15).

Duas decisões do Pablo nesta rodada: **uma conexão por conta** (D2), com o custo de 10
conexões e a pendência P3 do path no ETL; e **a stream `comment` como requisito da v1**
(D11), o que põe a Análise do App (P1) no caminho crítico. Criados `CLAUDE.md`,
`docs/docs.md`, `docs/metrics.md`, este plano, `.env.example`, `.gitignore` e `README.md`.
Scripts da descoberta ficaram no scratchpad da sessão, não no projeto.
