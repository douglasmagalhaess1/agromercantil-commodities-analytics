-- Indices para otimizacao das queries analiticas (Q6a, Q6b, Q6c)
-- e consultas frequentes do dashboard Streamlit.


-- === price_raw ===

-- Q6a (preco medio mensal com LAG): agrupa por produto + DATE_TRUNC(data_referencia)
-- e filtra preco > 0. Indice composto permite index scan no GROUP BY sem sort adicional.
-- Sem indice: Seq Scan em price_raw (~15ms para 10k linhas, cresce linear).
-- Com indice: Index Scan + evita sort no GROUP BY (~2-5ms, escala logaritmico).
CREATE INDEX IF NOT EXISTS idx_price_raw_produto_data
    ON price_raw (produto, data_referencia);

-- Q6b (top 5 produtos): filtra data_referencia >= CURRENT_DATE - '1 year' e preco > 0.
-- Indice em data_referencia permite range scan eficiente no WHERE temporal.
-- Sem indice: Seq Scan varrendo todas as linhas para filtrar por data.
-- Com indice: Index Scan apenas no range do ultimo ano (~60-70% menos I/O).
CREATE INDEX IF NOT EXISTS idx_price_raw_data_referencia
    ON price_raw (data_referencia)
    WHERE data_referencia IS NOT NULL;

-- Q6c (anomalias): JOIN em produto + filtro em preco. Cobre tambem a CTE
-- que calcula AVG/STDDEV agrupando por produto.
-- Sem indice: Hash Join + Seq Scan na tabela inteira para a CTE e o JOIN.
-- Com indice: Index Scan no JOIN por produto, reduz custo de lookup.
CREATE INDEX IF NOT EXISTS idx_price_raw_produto_preco
    ON price_raw (produto, preco);

-- Dashboard Streamlit: query principal faz SELECT * ORDER BY data_referencia.
-- Filtros por regiao geram WHERE regiao = '...'. Indice em regiao evita Seq Scan.
CREATE INDEX IF NOT EXISTS idx_price_raw_regiao
    ON price_raw (regiao);


-- === price_processed ===

-- Consultas analiticas juntam com commodity e regiao por FK.
-- Indices nas FKs evitam Seq Scan nos JOINs (PostgreSQL nao cria automaticamente).
CREATE INDEX IF NOT EXISTS idx_price_processed_commodity_id
    ON price_processed (commodity_id);

CREATE INDEX IF NOT EXISTS idx_price_processed_region_id
    ON price_processed (region_id);

-- Filtros temporais no dashboard e em queries ad-hoc.
CREATE INDEX IF NOT EXISTS idx_price_processed_data
    ON price_processed (data_referencia);


-- === price_curated ===

-- Tabela pequena (agregada), mas consultada com filtro por produto no dashboard.
CREATE INDEX IF NOT EXISTS idx_price_curated_produto
    ON price_curated (produto);