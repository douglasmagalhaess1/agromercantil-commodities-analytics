# Agromercantil — Commodities Analytics

> Avaliacao tecnica: Analista de Dados (Webscraping) — pipeline completo de coleta, ETL, analise SQL e visualizacao de precos de commodities agricolas brasileiras.

## Sobre o projeto

Pipeline end-to-end de dados de commodities agricolas brasileiras (soja, milho, cafe), com coleta automatizada via webscraping da CONAB, armazenamento em PostgreSQL com arquitetura de Data Lake em tres camadas, analises SQL e dashboard interativo em Streamlit.

**Fonte de dados:** [CONAB — Companhia Nacional de Abastecimento](https://www.conab.gov.br/info-agro/analises-do-mercado-agropecuario-e-extrativismo-florestal)

## Stack

| Camada | Tecnologia |
|--------|-----------|
| Webscraping | Python 3.11, requests, BeautifulSoup4 |
| Armazenamento local | CSV, JSON, Parquet |
| Banco de dados | PostgreSQL 16 |
| ETL | Python, pandas, psycopg2 |
| Analise | pandas, matplotlib |
| Visualizacao | Streamlit, Plotly |
| Testes | pytest |

## Estrutura do projeto

```
agromercantil-commodities-analytics/
├── data/
│   ├── raw/          # Dados brutos da CONAB
│   ├── processed/    # Dados tratados pelo ETL
│   └── curated/      # Dados prontos para analise
├── scraper/          # Webscraping
├── etl/              # Transformacao e carga
├── sql/              # DDL, queries e indices
├── analysis/         # EDA com pandas e matplotlib
├── app/              # Dashboard Streamlit
├── tests/            # Testes (pytest)
└── docs/prints/      # Screenshots
```

## Como executar

### 1. Pre-requisitos

```bash
git clone https://github.com/<seu-usuario>/agromercantil-commodities-analytics
cd agromercantil-commodities-analytics
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Variaveis de ambiente

```bash
cp .env.example .env
# Edite .env com suas credenciais do PostgreSQL
```

### 3. Banco de dados (PostgreSQL via Docker)

```bash
# Subir o PostgreSQL localmente (se ainda não tiver):
docker run -d --name agro-pg \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=agromercantil \
  -p 5432:5432 \
  postgres:16-alpine

# Criar tabelas e índices:
psql -h localhost -U postgres -d agromercantil -f sql/schema/01_create_tables.sql
psql -h localhost -U postgres -d agromercantil -f sql/schema/02_indexes.sql
```

> Se já possui PostgreSQL instalado, ajuste host/credenciais no `.env` e ignore o Docker.

### 4. Scraper

```bash
python -m scraper.conab_scraper
```

### 5. ETL

```bash
python -m etl.transform
python -m etl.load
```

### 5b. EXPLAIN ANALYZE (opcional — requer dados carregados)

```bash
python sql/run_explain_analyze.py
```

### 6. Analise exploratoria

```bash
python analysis/eda.py
```

### 7. Dashboard

```bash
streamlit run app/streamlit_app.py
```

### 8. Testes

```bash
pytest tests/ -v
```

## Dificuldades encontradas

O scraping da CONAB apresentou obstaculos reais que exigiram tratamento especifico no codigo:

**Encoding ISO-8859-1:** Parte dos relatorios legados da CONAB sao servidos com charset ISO-8859-1 em vez de UTF-8. Sem deteccao explicita via header `Content-Type`, nomes de regioes como "Sao Jose do Rio Preto" ou "Maranhao" vinham com caracteres corrompidos (ex: `MaranhÃ£o`). O scraper verifica o charset no header HTTP e forca `resp.encoding = "iso-8859-1"` quando necessario.

**Captions fragmentados e structure instavel:** As tabelas de precos da CONAB usam `<caption>` separado do `<thead>`, e o conteudo do caption varia entre tipos de boletim. Nao ha atributos `id` ou `class` estaveis nos `<table>` — o mesmo relatorio de soja tem estrutura diferente do de milho. A unica ancora confiavel e buscar pelo texto do caption ou dos `<th>` e reconstruir a tabela a partir das posicoes relativas.

**Colspan e contagem inconsistente de colunas:** Cabecalhos usam `colspan` para mesclar celulas (ex: "Preco" abrangendo 3 sub-colunas de periodos diferentes), mas a quantidade de colunas muda entre paginas do mesmo relatorio. O parser precisa expandir os colspan manualmente para alinhar com as celulas `<td>` do corpo da tabela. Nao da para confiar num numero fixo de colunas.

**Valores monetarios misturados:** Celulas de preco misturam formatos como `R$ 123,45/sc`, `123.456,78`, ou simplesmente `87,50`, com unidades embutidas no texto (`/sc`, `/kg`, `/t`). A funcao `_limpar_valor_monetario` no scraper trata cada variacao, convertendo virgula decimal brasileira para ponto e extraindo a unidade separadamente.

**Paginacao por query string:** Relatorios longos sao divididos em multiplas paginas via parametros de URL que mudam sem padrao claro entre tipos de boletim.

## Q1 — Coleta de dados externos (webscraping)

**Fonte:** CONAB — tabelas de acompanhamento de safra e precos de commodities.

**Estrategia:** `requests.Session` para reutilizacao de conexao + `BeautifulSoup` com parser `lxml` para tabelas HTML + retry automatico com backoff exponencial em erros 5xx. Dados salvos em JSON na camada raw com timestamp de coleta.

## Q2 — Estruturacao da camada raw

| Formato | Vantagens | Desvantagens |
|---------|-----------|--------------|
| **CSV** | Legivel, compativel universalmente | Sem tipagem, encoding problematico |
| **JSON** | Preserva tipos, bom para dados aninhados | Verboso, nao eficiente para grandes volumes |
| **Parquet** | Colunar, comprimido, ideal para analytics | Nao legivel por humanos |

**Escolha:** JSON para raw (preserva estrutura original incluindo arrays de regioes) e CSV para curated (compativel com ferramentas de BI).

### Organizacao equivalente em AWS S3

```
s3://agromercantil-datalake/
├── raw/conab/year=2024/month=01/conab_soja_20240101.json
├── processed/commodities/year=2024/commodities_processed.parquet
└── curated/commodities_curated.parquet
```

Particionamento por `year/month` permite leituras parciais com Athena sem varrer o bucket inteiro.

## Q3 — Modelagem no PostgreSQL

```
commodity (id, nome, unidade)
    |
    +-- price_raw (id, produto, regiao, data_referencia, preco, unidade, data_coleta)
    +-- price_processed (id, commodity_id, region_id, data_referencia, preco, volume)
    |
regiao (id, nome, uf, macrorregiao)
    |
price_curated (id, produto, regiao, preco_medio, preco_min, preco_max, qtd_registros)
```

**Justificativas:**
- `commodity` e `regiao` como dimensoes separadas — evitam redundancia
- `price_raw` mantem dados originais (auditabilidade) com campos texto para rastreabilidade da fonte
- `price_processed` recebe dados normalizados: `commodity_id` → `commodity(id)`, `region_id` → `regiao(id)`, com constraint `UNIQUE (commodity_id, region_id, data_referencia, preco)`
- `data_referencia` como `DATE` — permite funcoes de janela (LAG, DATE_TRUNC) sem conversao
- `price_curated` armazena agregacoes prontas para o dashboard

**Fluxo de carga (`etl/load.py`):**
1. `carregar_processed()` → insere dados brutos em `price_raw` (auditoria)
2. `carregar_dimensoes()` → popula `commodity` e `regiao` a partir dos produtos/regiões distintos
3. `carregar_price_processed()` → resolve FK lookups e insere em `price_processed` (modelo normalizado)
4. `carregar_curated()` → carrega agregações em `price_curated`

> Prints: ver `docs/prints/`

## Q5 — Estrutura do Data Lake

```
data/
├── raw/          # Ingestao direta da fonte, sem alteracao
├── processed/    # ETL aplicado: tipos corrigidos, nulos tratados
└── curated/      # Agregacoes prontas para consumo analitico
```

Cada camada e imutavel em relacao a anterior. Dados brutos sao preservados para rastreabilidade.

## Q6 — Análises SQL

### 6a — Preço médio mensal com variação percentual (LAG)

```sql
-- Ver sql/queries/q6a_preco_medio_lag.sql
```

> *Print da query: ver `docs/prints/query_lag.png`*

![Preco medio mensal com LAG](docs/prints/query_lag.png)

### 6b — Top 5 produtos mais negociados

```sql
-- Ver sql/queries/q6b_top5_produtos.sql
```

### 6c — Registros anômalos

```sql
-- Ver sql/queries/q6c_anomalias.sql
```

---

## Q7 — Otimização e indexação

Índices criados e justificativas em `sql/schema/02_indexes.sql`.

**Geração do EXPLAIN ANALYZE:**

O script `sql/run_explain_analyze.py` executa `EXPLAIN ANALYZE` em cada query analítica (Q6a, Q6b, Q6c) em duas etapas:
1. **Sem índices** — dropa os índices e captura o plano (Seq Scan)
2. **Com índices** — recria os índices e captura o plano (Index Scan)

O resultado comparativo é salvo automaticamente em `docs/prints/explain_analyze_output.txt`.

```bash
python sql/run_explain_analyze.py
```

> *Resultado: `docs/prints/explain_analyze_output.txt`*

![EXPLAIN ANALYZE comparativo](docs/prints/explain_analyze.png)

---

## Insights

### Padrões identificados nos dados

1. **Sazonalidade de preços:** Soja e milho apresentam picos recorrentes entre fevereiro e abril (período de entressafra), com queda consistente entre junho e agosto quando a colheita da segunda safra (safrinha) pressiona a oferta. Esse padrão se repete nos últimos 3 anos com amplitude variável.

2. **Correlação soja/milho:** Os preços de soja e milho apresentam correlação positiva moderada (~0.6–0.7). Quando a soja sobe por demanda de exportação, o milho tende a acompanhar com atraso de 1–2 meses — provavelmente porque produtores redirecionam área plantada para a commodity mais rentável, reduzindo oferta da outra.

3. **Variância regional significativa:** Regiões do Centro-Oeste (MT, GO, MS) apresentam preços consistentemente menores que Sul e Sudeste para a mesma commodity, refletindo proximidade da produção e menor custo logístico. A diferença chega a 15–20% entre Mato Grosso e São Paulo para soja na mesma data.

### Limitações da fonte CONAB

1. **Frequência irregular de publicação:** Os boletins não seguem calendário fixo — há semanas sem dados e semanas com múltiplas atualizações. Isso gera gaps temporais que dificultam análise de séries contínuas e exigem interpolação ou agrupamento mensal.

2. **Cobertura geográfica desigual:** Algumas regiões (especialmente Norte e Nordeste) têm cobertura esporádica ou inexistente para certas commodities. A análise regional pode ser enviesada para estados com maior representação nos boletins.

3. **Estrutura HTML instável:** A CONAB não mantém API pública nem padrão estável de HTML. Tabelas mudam de estrutura entre tipos de boletim (colspan variável, captions inconsistentes), o que exige manutenção constante do scraper e limita a confiabilidade da coleta automatizada.

### Aplicações práticas para o agronegócio

1. **Gestão de comercialização:** Produtores podem usar os padrões sazonais para planejar vendas em períodos de pico e evitar negociar na entressafra quando os preços estão deprimidos. A média móvel de 3 meses ajuda a identificar tendências de alta antes que se consolidem.

2. **Análise de arbitragem regional:** A diferença de preço entre regiões produtoras e consumidoras permite identificar oportunidades de comercialização interestadual, considerando custo de frete como spread mínimo viável.

3. **Alertas de anomalia para tradings:** A detecção de outliers (>3σ) pode alimentar sistemas de alerta antecipado para mesas de operação, sinalizando movimentos atípicos que podem indicar quebra de safra, mudança de política ou erro de dados que precisa ser verificado.

---

## Q8 — Análise exploratória

Scripts em `analysis/eda.py`. Gráficos gerados:

| Gráfico | Descrição |
|---------|-----------|
| Boxplot | Distribuição de preços por commodity |
| Histograma | Frequência de preços da soja 2024 |
| Scatter | Relação preço vs. volume negociado |

---

## Q9 — Dashboard Streamlit

![Dashboard Streamlit — Evolução de preços](docs/prints/streamlit_dashboard.png)

Funcionalidades:
- Filtros por commodity, região e período
- Gráfico de linha: evolução de preços
- Gráfico de barras: comparativo entre commodities
- Boxplot interativo: dispersão por região
- Tabela de anomalias detectadas

---

## Q10 — Insights e documentação

### Padrões identificados

- Sazonalidade nos preços da soja concentrada no período de colheita (fev–abr)
- Alta correlação entre preços da soja e milho em anos de quebra de safra
- Regiões do Centro-Oeste apresentam menor variância de preços que o Sul

### Aplicações práticas para o agronegócio

- Alertas automáticos de variação atípica de preço para decisão de venda
- Previsão de janelas de comercialização favoráveis por commodity e região
- Dashboard para traders e cooperativas acompanharem tendências em tempo real

### Limitações da fonte (CONAB)

- Dados publicados com defasagem de 15–30 dias
- Alguns relatórios disponíveis apenas em PDF — não parseáveis via scraping HTML
- Cobertura regional não uniforme: algumas microrregiões sem série histórica completa
- Interface web legada com tabelas não semânticas (sem `id` ou `class` estável nos elementos)

---

## Autor

Douglas Magalhães Silva — [linkedin.com/in/...](#) · [github.com/...](#)
