-- Deteccao de registros anomalos
--
-- Tres tipos de anomalia:
--   1) Preco negativo — erro de parsing ou dado corrompido na fonte
--   2) Preco nulo — falha na coleta (constraint NOT NULL impede no schema,
--      mas a query serve como validacao de seguranca)
--   3) Preco acima de 3 desvios padrao da media do produto — outlier estatistico
--
-- Threshold de 3 sigma: cobre 99.7% da distribuicao normal. Escolhido por ser
-- o padrao em controle de qualidade de dados. Valores entre 2 e 3 sigma podem
-- ser sazonais (entressafra), mas acima de 3 sigma quase sempre indica erro
-- de digitacao ou parsing incorreto da fonte.

WITH estatisticas AS (
    SELECT
        produto,
        AVG(preco)    AS media,
        STDDEV(preco) AS desvio
    FROM price_raw
    WHERE preco > 0
    GROUP BY produto
)
SELECT
    r.id,
    r.produto,
    r.regiao,
    r.data_referencia,
    r.preco,
    r.data_coleta,
    CASE
        WHEN r.preco < 0                              THEN 'preco_negativo'
        WHEN r.preco IS NULL                           THEN 'preco_nulo'
        WHEN r.preco > e.media + 3 * e.desvio         THEN 'outlier_acima_3sigma'
        WHEN r.preco < GREATEST(e.media - 3 * e.desvio, 0) THEN 'outlier_abaixo_3sigma'
    END AS tipo_anomalia,
    ROUND(e.media, 2)  AS media_produto,
    ROUND(e.desvio, 2) AS desvio_produto
FROM price_raw r
JOIN estatisticas e ON e.produto = r.produto
WHERE r.preco < 0
   OR r.preco IS NULL
   OR r.preco > e.media + 3 * e.desvio
   OR r.preco < GREATEST(e.media - 3 * e.desvio, 0)
ORDER BY r.produto, r.data_referencia;