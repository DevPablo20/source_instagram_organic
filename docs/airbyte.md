# Instagram Organic — guia de uso

Conector de conteúdo **orgânico** de contas profissionais do Instagram, com o
**impulsionado separado do orgânico** em toda métrica. Substitui o `source-instagram`
oficial.

Este guia é para quem configura o source no Airbyte e para quem consome as tabelas. O
contrato técnico detalhado está em [streams.md](https://github.com/DevPablo20/source_instagram_organic/blob/main/docs/streams.md); a lista crua de métricas, em
[metrics.md](https://github.com/DevPablo20/source_instagram_organic/blob/main/docs/metrics.md).

---

## 1. O que este conector entrega que o oficial não entrega

| Pergunta de negócio | Oficial | Este |
|---|---|---|
| Quanto deste alcance foi **pago**? | não responde | coluna `scope` em toda métrica |
| Quantas **visualizações** teve esta foto ou carrossel? | só pede para reels | todas as mídias |
| A **story** funcionou? | 7 métricas, sem navegação | 13 métricas, com `navigation` e `link_clicks` |
| O que o público está **comentando**? | não existe | stream `comment`, com thread e autor |
| Qual era o alcance real de um post de 2023? | relê e grava o número degradado | não consulta, e explica por quê |
| Quanto demora o sync? | cresce com a idade da conta | constante: 32s por conta por dia |

Medido na Texaco: o backfill de 288 dias traz 21.599 registros em 6min37s; o sync diário
seguinte traz **952 registros em 32,6s**.

---

## 2. Configuração

### Credencial

Token de **usuário de sistema** do Business Manager, sem expiração. Permissões no App:

| Permissão | Para quê |
|---|---|
| `instagram_basic` | conta, mídia, stories |
| `instagram_manage_insights` | todas as métricas |
| `pages_show_list` | descobrir a conta pela página ligada |
| `pages_read_engagement` | leitura da página ligada |
| `business_management` | ativos via Business Manager |
| `instagram_manage_comments` | **autor** do comentário |

> **A armadilha das duas portas.** O autor do comentário exige a permissão de App **e** a
> task *Atividade da comunidade* (`MODERATE`) na página ligada àquela conta. Com a permissão
> e sem a task, a API responde `200` e simplesmente **omite** `username` e `from_id` — sem
> erro, sem aviso. O `check` do conector detecta isso e avisa no log.

### Descobrir o id da conta

```
GET /v26.0/me/accounts?fields=name,instagram_business_account{id,username}
```

O id tem 17 dígitos e começa com `1784`. **Uma conta por conexão.**

### Campos

| Campo | Default | O que governa |
|---|---|---|
| `start_date` | — | corte histórico duro |
| `lookback_window_days` | 28 | até que idade a mídia é relida |
| `account_lookback_days` | 7 | até que idade o dia da conta é relido |
| `insights_max_age_days` | 700 | teto de idade para insights de mídia |
| `comment_window_days` | 90 | até que idade a mídia é vigiada por comentários |
| `include_replies` | true | respostas na mesma stream |
| `demographics_timeframe` | last_30_days | janela de cálculo da demografia |
| `api_version` | v26.0 | versão da Graph API |

As quatro listas de métricas (`account_metrics`, `media_metrics_feed`,
`media_metrics_reels`, `story_metrics`) ficam **vazias** no uso normal: vazio significa "a
lista validada contra a API".

> **`insights_max_age_days` não é economia, é integridade.** A Meta guarda insights de mídia
> por ~2 anos; depois disso não devolve erro, devolve número degradado. Numa publicação da
> Texaco o alcance era 1.666 e hoje a API devolve **81**; acima de ~900 dias as
> visualizações voltam **0**. Como o lake toma a leitura mais recente como valor atual,
> reler mídia velha troca o histórico bom por lixo. Os contadores de curtidas e comentários
> sobrevivem à retenção e continuam sendo coletados — por isso a stream `media_metrics`
> ignora este teto.

> Mudar `start_date`, qualquer janela ou as listas de métricas **exige Clear data** dos
> streams afetados.

### Conexões recomendadas, por conta

| Conexão | Streams | Agendamento |
|---|---|---|
| diária | tudo menos `story` e `story_insights` | 01:00 America/Sao_Paulo |
| stories | `story`, `story_insights` | de hora em hora |
| crise | `media`, `media_metrics`, `comment` | de hora em hora, sob demanda |

Stories precisam de conexão horária porque **só existem 24h**: em sync diário, a story
simplesmente não está lá quando o conector olha.

**Todas as conexões são Append.** `Overwrite` apaga os arquivos anteriores do stream — nas
stories isso destruiria justamente o histórico que a coleta horária existe para capturar. A
deduplicação acontece no bronze, pela chave natural de cada stream.

---

## 3. O modelo de escopo: orgânico, Facebook, pago e total

Toda stream de insight tem a coluna **`scope`**. A pergunta "quanto foi impulsionado?" é um
filtro, não uma fórmula.

| `scope` | Significa |
|---|---|
| `organic` | alcance e interação espontâneos no Instagram |
| `facebook` | reprodução no Facebook, por publicação cruzada |
| `paid` | impulsionamento |
| `total` | tudo somado — é o número que o Business Suite mostra |

O nome da métrica é normalizado: `views`, `facebook_views` e `total_views` viram a métrica
**`views`** em três escopos, e o `paid` fecha a conta. Linhas calculadas pelo conector
trazem `is_derived = true`.

Na conta o recorte é nativo (a API tem a dimensão `AD`). Na mídia ele é derivado
(`total_views − views − facebook_views`) e foi verificado em **227 de 227 mídias, sem um
único caso incoerente**.

Em `likes` e `comments` não existe equivalente de Facebook, então o `paid` derivado absorve
o Facebook junto com o anúncio. É uma aproximação declarada, não um erro silencioso.

**A escala disso:** das 227 mídias da Texaco no período, **171 foram impulsionadas**. A
maior tem 2.579 visualizações orgânicas e **2.024.379 pagas** — 99,9% do total. Hoje o lake
recebe só o orgânico, e sem dizer que é orgânico.

### Três regras que o consumo não pode violar

1. **`reach` não soma — nem entre escopos, nem entre dias.** É conta única de pessoas.
   Somar os recortes de um dia dá 187.850 contra 186.668 reais; somar 28 dias dá 1.399.918
   contra 1.055.049 reais, **1,33× de inflação**. Use a linha `scope: total`, e para
   período use `period = week` ou `days_28`, que a API entrega já deduplicados. Todas as
   outras métricas são aditivas por dia (`views` fecha em 1,0000).
2. **Valor pode ser negativo.** `saves` é líquido de "desguardar"; já foi medido **−1**.
3. **Toda métrica de mídia e de story é `lifetime` acumulada.** O valor atual é o último
   `snapshot_date`; a diferença entre dois snapshots é o ganho do dia. Nunca some snapshots.

---

## 4. As streams

### Família conta

#### `account` — identidade e curva de seguidores
Uma foto por dia do perfil. Responde "quantos seguidores eu tenho e quanto cresci".
**Métricas:** `followers_count`, `follows_count`, `media_count`, mais identificação.
**Consumo:** dimensão de todas as outras streams, por `business_account_id`. O `page_id`
liga ao `facebook_organic`. A diferença entre duas fotos é o ganho líquido do dia.
*`media_count` não bate com o total de publicações de `/media` — para contar posts, use a
stream `media`.*

#### `account_insights` — desempenho diário da conta, com o pago separado
A stream mais rica. Formato longo: `metric` × `period` × `scope` × `breakdown`.

| Métrica | Significado |
|---|---|
| `reach` | contas únicas alcançadas (também em `period` week e days_28) |
| `follower_count` | novos seguidores no dia |
| `views` | visualizações de todo o conteúdo |
| `likes`, `comments`, `shares`, `saves` | interações |
| `replies` | respostas a stories |
| `reposts` | republicações |
| `total_interactions` | soma das interações com conteúdo |
| `accounts_engaged` | contas únicas que interagiram |
| `profile_views` | visitas ao perfil |
| `website_clicks` | toques na ligação do site |
| `profile_links_taps` | toques nos botões de contato (recorte `contact_button_type`) |
| `follows_and_unfollows` | ganhos e perdas (recorte `follow_type`) |

**Recortes:** `media_product_type` separa `POST`, `REEL`, `CAROUSEL_CONTAINER`, `STORY` e
**`AD`** — é a origem do `scope: paid`. `follow_type` separa seguidor de não-seguidor.
**Consumo:** pivotar por `metric` + `scope`. Somar dias para total de período, **exceto
`reach`**.

#### `account_demographics` — quem é a audiência
Idade, gênero, país e cidade dos seguidores, uma foto por dia.
**Consumo:** comparar fotos distantes mostra deslocamento de audiência. País e cidade vêm
**truncados nos 45 maiores**, então não somam 100% — use `followers_count` da `account`
como denominador, nunca a soma das linhas.

### Família conteúdo

#### `media` — o catálogo de publicações
A dimensão do conteúdo: uma linha por publicação, gravada uma vez e nunca reescrita. Cobre
feed, carrossel e reel; reel é `media_product_type = REELS`.
**Campos:** `caption`, `permalink`, `shortcode`, `timestamp`, `media_type`,
`media_product_type`, `is_comment_enabled`, `username`, `alt_text`.
**Consumo:** junta com as outras por `media_id`. `is_comment_enabled` importa para o
monitoramento — post com comentários desligados não é "post sem reclamação".

#### `media_metrics` — o placar de cada post ao longo do tempo
Curtidas e comentários datados, por publicação.
**Métricas:** `like_count`, `comments_count` — ambos **orgânicos** (`like_count` bateu com o
insight `likes` em 80 de 81 mídias medidas).
**Consumo:** último snapshot é o valor atual; a diferença entre snapshots é o ganho do dia.
É a única stream que **continua funcionando em mídia com mais de 2 anos**, porque contador
sobrevive à retenção que destrói alcance e visualizações.

#### `media_insights` — desempenho de cada publicação
Formato longo, com `scope`. Só mídia dentro da janela e abaixo do teto de idade.

**FEED (imagem e carrossel), 15 métricas:**

| Métrica | Significado |
|---|---|
| `reach` | contas únicas alcançadas |
| `views` | visualizações (orgânica, Facebook, paga e total) |
| `likes`, `comments` | interações (orgânica, paga e total) |
| `saved` | vezes que foi guardado |
| `shares` | compartilhamentos |
| `follows` | seguidores ganhos a partir da publicação |
| `profile_visits` | visitas ao perfil a partir da publicação |
| `profile_activity` | ações no perfil depois da publicação (recorte `action_type`) |
| `total_interactions` | soma das interações |
| `reposts` | republicações |

**REELS, 16 métricas:** as mesmas **menos** `follows`, `profile_visits` e
`profile_activity`, **mais**:

| Métrica | Significado |
|---|---|
| `ig_reels_avg_watch_time` | tempo médio assistido, em **milissegundos** |
| `ig_reels_video_view_total_time` | tempo total assistido, em ms, com repetições |
| `reels_skip_rate` | **percentual** que sai nos 3 primeiros segundos (ex.: 55,9) |
| `crossposted_views` | visualizações orgânicas somando Instagram e Facebook |

#### `story` e `story_insights` — o que esteve no ar, e se funcionou
`story` é o inventário; sem a coleta horária não há histórico nenhum. `story_insights` traz
13 métricas, e a **última leitura antes de expirar é o número final**.

| Métrica | Significado |
|---|---|
| `reach`, `views` | alcance e visualizações |
| `navigation` | **o que o espectador fez**: `tap_forward`, `tap_back`, `tap_exit`, `swipe_forward` |
| `link_clicks` | cliques na ligação — a conversão da story |
| `replies` | respostas por direct |
| `shares`, `follows`, `profile_visits`, `profile_activity`, `reposts`, `total_interactions` | interações |

`navigation` e `link_clicks` são as duas que dizem se a story prendeu a atenção, e são
justamente as que o conector oficial não traz.
*Story com menos de 5 espectadores não tem métrica: a API recusa e a story é gravada assim
mesmo. Isso é normal.*

### Família conversa

#### `comment` — o que o público está dizendo
A stream que substitui o social listening externo. Comentários e respostas na mesma tabela.

| Campo | Significado |
|---|---|
| `text`, `timestamp` | o conteúdo e quando |
| `parent_id` | preenchido = é resposta; monta a thread |
| `like_count`, `hidden` | engajamento e moderação |
| `username`, `from_id` | **autor** (exige permissão + task `MODERATE`) |

**Consumo:** volume por dia por conta é o detector de crise; o texto alimenta classificação
e sentimento no silver. `from_id` é o IGSID — não resolve como URL de perfil.

No backfill da Texaco: 3.335 comentários de 209 publicações, 1.233 respostas, 1.232 autores
distintos.

---

## 5. Como cada stream roda

| Stream | Modo | Cursor | Conexão |
|---|---|---|---|
| `account` | Full refresh + Append | — | diária |
| `account_insights` | **Incremental** + Append | `date` | diária |
| `account_demographics` | Full refresh + Append | — | diária |
| `media` | **Incremental** + Append | `timestamp` | diária |
| `media_metrics` | **Incremental** + Append | `snapshot_date` | diária + crise |
| `media_insights` | **Incremental** + Append | `snapshot_date` | diária + crise |
| `story` | Full refresh + Append | — | **horária** |
| `story_insights` | Full refresh + Append | — | **horária** |
| `comment` | **Incremental** + Append | `timestamp` | diária + crise |

"Incremental" aqui é sobre **o que o conector lê**: o state limita a janela de leitura. A
gravação é sempre Append, porque o destino S3/MinIO não faz deduplicação.

"Full refresh + Append" **não é Overwrite**: nada é apagado, cada execução acrescenta uma
foto nova datada por `snapshot_date`.

---

## 6. Limites conhecidos

- **Insights de mídia morrem em ~2 anos.** Não é limitação do conector, é da plataforma.
- **`reach` nunca pode ser somado** — ver a regra 1 da seção 3.
- **Demografia vem truncada nos 45 maiores** em país e cidade.
- **`media_count` da conta não bate** com o total de `/media` (Texaco: 1.737 × 1.479). Não
  investigado; use a stream `media` para contar publicações.
- **Menção de terceiro não entra na v1.** A edge `/tags` já responde com a permissão atual,
  mas a stream ainda não existe.
- **O autor do comentário depende de task por página**, não só da permissão do App.
