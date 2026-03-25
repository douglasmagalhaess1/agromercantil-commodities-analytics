-- Preco medio mensal por commodity com variacao percentual (LAG)
--
-- DATE_TRUNC('month', ...) foi escolhido porque os dados da CONAB chegam em
-- datas irregulares (dias uteis, sem padrao fixo). Truncar para mes garante
-- agrupamento uniforme e permite comparacao direta entre periodos consecutivos
-- sem gaps artificiais que ocorreriam com agrupamento semanal ou diario.

WITH media_mensal AS (
    SELECT
        produto,
        DATE_TRUNC('month', data_referencia) AS mes,
        ROUND(AVG(preco), 2)                 AS preco_medio,
        COUNT(*)                              AS qtd_registros
    FROM price_raw
    WHERE data_referencia IS NOT NULL
      AND preco > 0
    GROUP BY produto, DATE_TRUNC('month', data_referencia)
)
SELECT
    produto,
    mes,
    preco_medio,
    qtd_registros,
    LAG(preco_medio) OVER (PARTITION BY produto ORDER BY mes) AS preco_medio_anterior,
    ROUND(
        (preco_medio - LAG(preco_medio) OVER (PARTITION BY produto ORDER BY mes))
        / NULLIF(LAG(preco_medio) OVER (PARTITION BY produto ORDER BY mes), 0) * 100,
        2
    ) AS variacao_percentual
FROM media_mensal
ORDER BY produto, mes;