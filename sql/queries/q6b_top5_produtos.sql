-- Top 5 produtos mais negociados no ultimo ano
--
-- Criterio de "mais negociado": quantidade de registros de preco no periodo.
-- A CONAB nao publica volume financeiro por transacao, entao o numero de
-- cotacoes registradas e o melhor proxy disponivel para atividade de mercado.
-- Produtos com mais cotacoes em regioes distintas indicam maior liquidez.

SELECT
    produto,
    COUNT(*)                        AS total_registros,
    COUNT(DISTINCT regiao)          AS regioes_distintas,
    ROUND(AVG(preco), 2)            AS preco_medio,
    ROUND(MIN(preco), 2)            AS preco_min,
    ROUND(MAX(preco), 2)            AS preco_max
FROM price_raw
WHERE data_referencia >= CURRENT_DATE - INTERVAL '1 year'
  AND preco > 0
GROUP BY produto
ORDER BY total_registros DESC
LIMIT 5;