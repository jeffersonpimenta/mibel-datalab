# MIBEL Datalab

Repositório para recolha, armazenamento e análise dos dados do mercado ibérico de eletricidade (MIBEL). Inclui scripts de scraping / ingestão, estrutura da base de dados e exemplos de consultas.

---

## Estrutura do Repositório

```
mibel-datalab/
├── Clickhouse/           # scripts / schemas específicos para ClickHouse
├── Python/               # scripts para scraping, preparação, etc.
├── src/                  # código fonte da interface
├── docker-compose.yml
├── LICENSE
└── README.md  
```

---

## Estrutura da Base de Dados (ClickHouse)

A base de dados principal tem a tabela `ofertas`, com o seguinte schema:

```sql
CREATE TABLE ofertas (
  id UInt64,
  periodo UInt32,
  data Date,
  pais LowCardinality(String),
  tipo_oferta LowCardinality(String),
  volume Float32,
  preco Float32,
  status LowCardinality(String),
  tipologia LowCardinality(String),
  arquivo String
) ENGINE = MergeTree()
ORDER BY (data, pais, tipo_oferta);
```

Colunas:

- `id`: identificador único da oferta  
- `periodo`: período do dia (1‑25)  
- `data`: data da oferta (YYYY-MM-DD)
- `pais`: país ("ES", "PT" ou "MI")  
- `tipo_oferta`: compra ("C") ou venda ("V")  
- `volume`: volume da oferta  
- `preco`: preço associado à oferta  
- `status`: estado da oferta  
- `tipologia`: categoria / tipologia da oferta  
- `arquivo`: nome do ficheiro de origem da oferta

---

## Ingestão de Dados / Importar CSV para ClickHouse

Assumindo que já tens os ficheiros CSV gerados via scraping / preparação, podes importar para a tabela `ofertas` com:

```bash
docker exec -it <nome_container_clickhouse> bash

clickhouse-client --query="
  INSERT INTO ofertas FORMAT CSVWithNames
" < /caminho/para/csv/curvas_mercado.csv
```

Substitui `<nome_container_clickhouse>` pelo nome ou ID do teu container Docker, e `/caminho/para/csv/curvas_mercado.csv` pelo caminho no sistema de ficheiros onde o CSV está.

---

## Exemplo de Query SQL: Clearing Price

O clearing price de um determinado dia e período é o preço mínimo de venda que satisfaz toda a demanda de compra. Aqui vai um exemplo de query para obter o clearing price:

```sql
WITH
  total_buy AS (
    SELECT
      data,
      periodo,
      SUM(volume) AS vol_compra
    FROM default.ofertas
    WHERE tipo_oferta = 'C'
      AND data = :data         -- substituir por data desejada ('YYYY-MM-DD')
      AND periodo = :periodo   -- substituir por período desejado
    GROUP BY data, periodo
  ),
  sells AS (
    SELECT
      data,
      periodo,
      preco,
      SUM(volume) AS vol_venda
    FROM default.ofertas
    WHERE tipo_oferta = 'V'
      AND data = :data
      AND periodo = :periodo
    GROUP BY data, periodo, preco
    ORDER BY data, periodo, preco ASC
  ),
  sell_cum AS (
    SELECT
      data,
      periodo,
      preco,
      vol_venda,
      SUM(vol_venda) OVER (
        PARTITION BY data, periodo
        ORDER BY preco ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS vol_vendido_acumulado
    FROM sells
  )
SELECT
  s.data,
  s.periodo,
  s.preco AS clearing_price,
  s.vol_vendido_acumulado AS volume_vendido_no_clearing
FROM sell_cum s
JOIN total_buy t
  ON s.data = t.data
  AND s.periodo = t.periodo
WHERE s.vol_vendido_acumulado >= t.vol_compra
ORDER BY s.preco
LIMIT 1;
```

- `:data` e `:periodo` são parâmetros que deves substituir para o dia e período que queres calcular.  
- O resultado dá o **preço de equilíbrio** e o volume vendido naquele preço.

---

## Como usar / Deploy

1. Clona este repositório:

   ```bash
   git clone https://github.com/jeffersonpimenta/mibel-datalab.git
   ```

2. Prepara o ambiente, por exemplo via Docker / Docker Compose se necessário para o clickhouse e para o php (ver `docker-compose.yml`).  

3. Corre o(s) script(s) de scraping / preparação (em `Python/`) para raspar os dados do site do OMIE.  

	```bash
	docker-compose up
	```

4. Usa a interface web para análises.

	```http
	http://localhost:8088/
	```

---


## Licença

Este projeto está licenciado sob a **CC BY-NC 4.0**. (ver ficheiro `LICENSE`)

---

## Contatos

Para questões, sugestões ou contributos, podes abrir *issues* no GitHub ou contactar diretamente o autor.
