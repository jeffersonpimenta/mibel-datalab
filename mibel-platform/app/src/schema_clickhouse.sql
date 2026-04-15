-- ClickHouse Schema for MIBEL Bid Analyzer Platform
-- Execute via migrate.php or manually via HTTP API

CREATE DATABASE IF NOT EXISTS mibel;

-- Raw bids data from OMIE ZIP files
CREATE TABLE IF NOT EXISTS mibel.bids_raw (
    data_ficheiro   Date,
    ficheiro_nome   String,
    zip_nome        String,
    hora_raw        String,         -- "1"-"24" ou "H1Q1"-"H24Q1"
    hora_num        UInt8,          -- sempre 1-24 normalizado
    periodo_formato String,         -- "NUM" ou "HxQy"
    pais            String,
    tipo_oferta     FixedString(1), -- "C" ou "V"
    unidade         String,
    energia         Float64,
    precio          Float64,
    ingestao_ts     DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(data_ficheiro)
ORDER BY (data_ficheiro, hora_num, pais, tipo_oferta, unidade);

-- Clearing results from substitution analysis
CREATE TABLE IF NOT EXISTS mibel.clearing_substituicao (
    job_id                  String,
    data_ficheiro           String,
    data_date               Date,
    hora_raw                String,
    hora_num                UInt8,
    pais                    String,
    preco_clearing_orig     Nullable(Float64),
    volume_clearing_orig    Nullable(Float64),
    preco_clearing_sub      Nullable(Float64),
    volume_clearing_sub     Nullable(Float64),
    delta_preco             Nullable(Float64),
    n_bids_substituidos     UInt32,
    created_at              DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(data_date)
ORDER BY (job_id, data_date, hora_num, pais);

-- Detailed logs of substituted bids
CREATE TABLE IF NOT EXISTS mibel.clearing_substituicao_logs (
    job_id          String,
    data_ficheiro   String,
    data_date       Date,
    hora_raw        String,
    hora_num        UInt8,
    pais            String,
    unidade         String,
    categoria       String,
    escalao_preco   Float64,
    preco_original  Float64,
    energia_mw      Float64,
    created_at      DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(data_date)
ORDER BY (job_id, data_date, hora_num, pais, unidade);

-- Worker execution logs
CREATE TABLE IF NOT EXISTS mibel.worker_logs (
    job_id        String,
    nivel         String,   -- "OK", "ERRO", "INFO", "AVISO"
    mensagem      String,
    ts            DateTime64(3) DEFAULT now64()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDate(ts))
ORDER BY (job_id, ts);

-- Optimization results: base clearing vs optimal PRE clearing
CREATE TABLE IF NOT EXISTS mibel.clearing_otimizacao (
    job_id                      String,
    data_ficheiro               String,
    data_date                   Date,
    hora_raw                    String,
    hora_num                    UInt8,
    pais                        String,
    preco_clearing_orig         Nullable(Float64),
    volume_clearing_orig        Nullable(Float64),
    preco_clearing_base         Nullable(Float64),
    volume_clearing_base        Nullable(Float64),
    preco_clearing_opt          Nullable(Float64),
    volume_clearing_opt         Nullable(Float64),
    vol_pre_despachado_base     Float64,
    lucro_pre_base              Float64,
    vol_pre_despachado_opt      Float64,
    lucro_pre_opt               Float64,
    delta_preco                 Nullable(Float64),
    delta_vol_pre_despachado    Float64,
    delta_lucro_pre             Float64,
    delta_lucro_pre_pct         Nullable(Float64),
    vol_pre_removido_opt        Float64,
    n_bids_pre_removidos        UInt32,
    unidades_pre_despachadas    String,
    n_cenarios_testados         UInt32,
    created_at                  DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(data_date)
ORDER BY (job_id, data_date, hora_num, pais);

-- Scenario logs: each tested price level per (hora, pais, date)
CREATE TABLE IF NOT EXISTS mibel.clearing_otimizacao_logs (
    job_id           String,
    data_ficheiro    String,
    data_date        Date,
    hora_raw         String,
    hora_num         UInt8,
    pais             String,
    cenario          String,
    preco_clearing   Nullable(Float64),
    volume_clearing  Nullable(Float64),
    lucro_pre        Float64,
    n_bids_removidos UInt32,
    vol_removido     Float64,
    created_at       DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(data_date)
ORDER BY (job_id, data_date, hora_num, pais, cenario);

-- Unit classification mapping loaded from LISTA_UNIDADES.csv (OMIE)
-- Populated by scripts/unidades/carrega_unidades_ch.py
-- Used by substituicao_worker.py to classify bid units by CODIGO
CREATE TABLE IF NOT EXISTS mibel.unidades (
    codigo          String,
    descricao       String,
    agente          String,
    tipo_unidad     String,
    zona_frontera   String,
    tecnologia      String,
    regime          String,   -- PRE, PRO, CONSUMO, COMERCIALIZADOR, GENERICA, PORFOLIO, OUTRO
    categoria       String,   -- ex: SOLAR_FOT_ES, EOLICA_PT, CICLO_COMBINADO_ES
    atualizado_em   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(atualizado_em)
ORDER BY (codigo);
