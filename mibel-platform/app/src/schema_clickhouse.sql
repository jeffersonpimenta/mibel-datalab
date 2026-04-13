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
    periodo_formato FixedString(3), -- "NUM" ou "HxQy"
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
