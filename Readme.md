CREATE TABLE ofertas (id UInt64, periodo UInt32, data Date, pais LowCardinality(String), tipo_oferta LowCardinality(String), volume Float32, preco Float32, status LowCardinality(String), tipologia LowCardinality(String), arquivo String) ENGINE = MergeTree() ORDER BY (data, pais, tipo_oferta);



docker exec -it b20e6f27aa69c5db3509d3faaf5aca27fa7ccb31969786c6ecd968127be64430 bash

clickhouse-client  --query="INSERT INTO ofertas FORMAT CSVWithNames" < /csv/curvas_mercado.csv



/* ----------------------------------------------
   PRELIMINAR:  total de compra do dia e período
---------------------------------------------- */
WITH total_buy AS (
    SELECT data,
           periodo,
           SUM(volume) AS vol
    FROM default.ofertas
    WHERE tipo_oferta = 'C'
      AND data = :data          -- <‑ substitua pela data desejada (YYYY-MM-DD)
      AND periodo = :periodo    -- <‑ substitua pelo período (ex.: 1, 2, …)
    GROUP BY data, periodo
),

/* ----------------------------------------------
   VENDAS agrupadas por preço, dia e período
---------------------------------------------- */
sells AS (
    SELECT data,
           periodo,
           preco,
           SUM(volume) AS vol
    FROM default.ofertas
    WHERE tipo_oferta = 'V'
      AND data = :data
      AND periodo = :periodo
    GROUP BY data, periodo, preco
    ORDER BY data, periodo, preco ASC
),

/* ----------------------------------------------
   Soma cumulativa das vendas (por dia e período)
---------------------------------------------- */
sell_cum AS (
    SELECT data,
           periodo,
           preco,
           SUM(vol) OVER (
               PARTITION BY data, periodo
               ORDER BY preco ASC
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS cum_vol
    FROM sells
)

/* ----------------------------------------------
   Retorna o clearing price (primeiro preço que cobre a demanda)
---------------------------------------------- */
SELECT s.data,
       s.periodo,
       s.preco          AS clearing_price,
       s.cum_vol        AS vol_sold_at_closing
FROM sell_cum s
JOIN total_buy t ON s.data = t.data AND s.periodo = t.periodo
WHERE s.cum_vol >= t.vol                -- o volume acumulado já cobre a demanda
ORDER BY s.preco
LIMIT 1;
