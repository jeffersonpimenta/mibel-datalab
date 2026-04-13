-- SQLite Schema for MIBEL Bid Analyzer Platform
-- Job queue and ingestion tracking

-- Jobs table for study queue management
CREATE TABLE IF NOT EXISTS jobs (
    id          TEXT PRIMARY KEY,   -- UUID gerado em PHP
    tipo        TEXT NOT NULL,      -- "substituicao" ou "otimizacao"
    data_inicio TEXT NOT NULL,
    data_fim    TEXT NOT NULL,
    observacoes TEXT DEFAULT '',
    workers_n   INTEGER DEFAULT 4,
    status      TEXT DEFAULT 'PENDING', -- PENDING, RUNNING, DONE, FAILED
    resultado   TEXT DEFAULT '',    -- JSON com resumo ao terminar
    erro        TEXT DEFAULT '',
    created_at  TEXT DEFAULT (datetime('now')),
    started_at  TEXT,
    finished_at TEXT
);

-- Track ingested bid files to avoid re-processing
CREATE TABLE IF NOT EXISTS bids_ingeridos (
    data_ficheiro TEXT PRIMARY KEY,
    n_bids        INTEGER,
    ingerido_em   TEXT DEFAULT (datetime('now'))
);
