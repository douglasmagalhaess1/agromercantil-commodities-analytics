-- ============================================================
-- Índices otimizados para queries analíticas (B-Tree)
-- Compatível com: Q6a (LAG), Q6b (Top5), Q6c (Anomalias), Dashboard
-- ============================================================

-- === price_raw ===

-- Q6a: GROUP BY produto, DATE_TRUNC(data_referencia) — evita sort adicional
CREATE INDEX IF NOT EXISTS idx_price_raw_produto_data
    ON price_raw (produto, data_referencia);

-- Q6b: WHERE data_referencia >= CURRENT_DATE - '1 year' — range scan temporal
CREATE INDEX IF NOT EXISTS idx_price_raw_data_referencia
    ON price_raw (data_referencia)
    WHERE data_referencia IS NOT NULL;

-- Q6c: JOIN em produto + filtro em preco (CTE de AVG/STDDEV)
CREATE INDEX IF NOT EXISTS idx_price_raw_produto_preco
    ON price_raw (produto, preco);

-- Dashboard: filtro WHERE regiao = '...'
CREATE INDEX IF NOT EXISTS idx_price_raw_regiao
    ON price_raw (regiao);


-- === price_processed ===

-- FK indexes (PostgreSQL não cria automaticamente em FKs)
CREATE INDEX IF NOT EXISTS idx_price_processed_commodity_id
    ON price_processed (commodity_id);

CREATE INDEX IF NOT EXISTS idx_price_processed_region_id
    ON price_processed (region_id);

-- Filtros temporais no dashboard e queries ad-hoc
CREATE INDEX IF NOT EXISTS idx_price_processed_data
    ON price_processed (data_referencia);


-- === price_curated ===

-- Filtro por produto no dashboard
CREATE INDEX IF NOT EXISTS idx_price_curated_produto
    ON price_curated (produto);