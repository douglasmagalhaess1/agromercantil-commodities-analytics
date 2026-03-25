# Estrutura do Repositório

```
agromercantil-commodities-analytics/
│
├── README.md
├── requirements.txt
├── .env.example
├── .gitignore
│
├── data/
│   ├── raw/                          # Camada raw — dados brutos como coletados
│   │   ├── conab_soja_2024.json
│   │   ├── conab_milho_2024.json
│   │   ├── conab_cafe_2024.json
│   │   └── conab_soja_2024.csv       # mesmo dado, formato alternativo
│   │
│   ├── processed/                    # Camada processed — dados tratados pelo ETL
│   │   ├── commodities_processed.csv
│   │   └── anomalias_detectadas.csv
│   │
│   └── curated/                      # Camada curated — prontos para análise/BI
│       └── commodities_curated.csv
│
├── scraper/
│   ├── __init__.py
│   ├── conab_scraper.py              # Script principal de webscraping
│   └── utils.py                      # Helpers: parse de tabelas, paginação
│
├── etl/
│   ├── __init__.py
│   ├── transform.py                  # Limpeza, padronização, correção de tipos
│   └── load.py                       # Carga no PostgreSQL (raw → processed)
│
├── sql/
│   ├── schema/
│   │   ├── 01_create_tables.sql      # DDL: tabelas normalizadas
│   │   └── 02_indexes.sql            # Índices sugeridos (Q7)
│   │
│   └── queries/
│       ├── q6a_preco_medio_lag.sql   # Preço médio mensal com variação LAG
│       ├── q6b_top5_produtos.sql     # Top 5 produtos mais negociados
│       └── q6c_anomalias.sql         # Detecção de registros anômalos
│
├── analysis/
│   ├── eda.py                        # Análise exploratória (Q8)
│   └── plots/                        # Gráficos gerados (para o README)
│       ├── boxplot_precos.png
│       ├── histograma_soja.png
│       └── scatter_preco_volume.png
│
├── app/
│   └── streamlit_app.py              # Dashboard Streamlit (Q9)
│
├── tests/
│   ├── test_scraper.py               # Testes do scraper
│   └── test_etl.py                   # Testes do ETL
│
└── docs/
    └── prints/                       # Screenshots para o README
        ├── streamlit_dashboard.png
        ├── query_lag.png
        ├── query_top5.png
        └── explain_analyze.png
```
