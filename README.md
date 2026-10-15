# source-instagram-organic

Conector Airbyte (Python CDK) para conteúdo orgânico de contas profissionais do Instagram —
conta, insights, demografia, mídia (feed, carrossel e reels), stories e comentários — com
janela de extração configurável e snapshot datado. Substitui o `source-instagram` oficial,
que só tem incremental em `user_insights` e relê as 2.722 mídias das contas a cada sync.

**Guia de uso (streams, métricas e configuração): [docs/airbyte.md](docs/airbyte.md).**

Escopo, regras e decisões: [CLAUDE.md](CLAUDE.md) ·
[docs/docs.md](docs/docs.md) · [docs/metrics.md](docs/metrics.md) ·
[docs/plans/connector-v1.md](docs/plans/connector-v1.md)

## Streams

| Stream | Modo | Chave | Observação |
|---|---|---|---|
| `account` | full refresh | `business_account_id + snapshot_date` | foto diária |
| `account_insights` | incremental | `business_account_id + metric + breakdown + date` | série nativa de 30 em 30 dias; as 13 métricas `total_value` dia a dia, com `AD` separado do orgânico |
| `account_demographics` | full refresh | `business_account_id + metric + breakdown + snapshot_date` | idade, gênero, país e cidade dos seguidores |
| `media` | incremental | `media_id` | append-once; feed, carrossel e reel na mesma stream |
| `media_metrics` | incremental | `media_id + snapshot_date` | `like_count` e `comments_count` |
| `media_insights` | incremental | `media_id + metric + breakdown + snapshot_date` | lista por `media_product_type`; só mídia na janela e com menos de `insights_max_age_days` |
| `story` | full refresh | `story_id + snapshot_date` | exige sync horário: story expira em 24h |
| `story_insights` | full refresh | `story_id + metric + breakdown + snapshot_date` | 13 métricas, incluindo `navigation` e `link_clicks` |
| `comment` | incremental | `comment_id` | respostas por field expansion; **espera `instagram_manage_comments`** |

## Configuração

Token de **usuário de sistema** do Business Manager. Diferente do `source_facebook_organic`,
não há token a derivar: o token de System User responde em todas as edges.

| Campo | Default |
|---|---|
| `access_token`, `instagram_business_account_id`, `start_date` | obrigatórios |
| `lookback_window_days` | 28 |
| `insights_max_age_days` | 700 |
| `comment_window_days` | 90 |
| `include_replies` | true |
| `demographics_timeframe` | last_30_days |
| `api_version` | v26.0 |
| `account_metrics`, `media_metrics_feed`, `media_metrics_reels`, `story_metrics` | listas validadas em `docs/metrics.md` |

## Por que não usar o oficial

| | Oficial | Este |
|---|---|---|
| Mídia lida por sync | 2.722 (todas) | 77 (janela de 28 dias) |
| Insights de mídia | ~33 min | ~8s (janela + batch de 50) |
| `views` de imagem e carrossel | não pede | sim |
| Orgânico × pago na conta | não pede o breakdown | `media_product_type` separa `AD` |
| Métricas `total_value` da conta | nenhuma das 13 | todas |
| Métricas de story | 7 | 13 |
| Comentários | não existem | stream própria |
| Insight de mídia com +2 anos | relê e grava o valor degradado | não consulta |

## Medido na Texaco (`start_date` 2026-01-01)

| | Backfill | Sync diário (com state) |
|---|---|---|
| Registros | 21.599 | **952** |
| Duração | 6min37s | **32,6s** |

Na segunda execução a janela de conta cai para 8 dias, a de mídia para 20 mídias, e 75
mídias são puladas pelo gate de `comments_count`.

## Desenvolvimento

O `python3` do host é 3.12 e está sem `ensurepip`; a venv usa o 3.11 do pyenv.

```bash
/home/pablo-notebook/.pyenv/versions/3.11.15/bin/python -m venv .venv
.venv/bin/pip install -e .
cp .env.example .env            # preencha; secrets/config.json é gerado a partir dele
.venv/bin/python main.py spec
.venv/bin/python main.py check --config secrets/config.json
.venv/bin/python main.py discover --config secrets/config.json
.venv/bin/python main.py read --config secrets/config.json --catalog catalog.json
```

## Publicação

```bash
docker build -t devpablo20/source-instagram-organic:0.1.1 -t devpablo20/source-instagram-organic:latest .
```

```bash
docker push devpablo20/source-instagram-organic:0.1.1
```

No Airbyte: Settings → Sources → New connector → nome `Instagram Organic`, imagem
`devpablo20/source-instagram-organic`, tag `0.1.0`, URL da documentação deste repositório.
