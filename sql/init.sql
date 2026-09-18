CREATE TABLE IF NOT EXISTS parse_jobs (
    id              BIGSERIAL PRIMARY KEY,
    file_seq_id     TEXT NOT NULL UNIQUE,         -- idempotency key (fileSeqId / generated)
    event_id        TEXT,
    recon_id        TEXT,
    rule_id         TEXT,
    source_name     TEXT,
    source_type     TEXT NOT NULL,                -- csv | excel | pdf | superset
    file_path       TEXT,
    engine          TEXT,                         -- native | textract | pandas | superset
    status          TEXT NOT NULL DEFAULT 'RECEIVED', -- RECEIVED|PROCESSING|COMPLETED|FAILED
    batch_size      INT  NOT NULL,
    total_records   BIGINT DEFAULT 0,
    total_batches   INT DEFAULT 0,
    warnings        JSONB DEFAULT '[]'::jsonb,
    error           TEXT,
    request         JSONB,                        -- original kafka / api payload
    created_at      TIMESTAMPTZ DEFAULT now(),
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_jobs_recon  ON parse_jobs(recon_id);
CREATE INDEX IF NOT EXISTS ix_jobs_status ON parse_jobs(status);

CREATE TABLE IF NOT EXISTS parsed_batches (
    id              BIGSERIAL PRIMARY KEY,
    job_id          BIGINT NOT NULL REFERENCES parse_jobs(id) ON DELETE CASCADE,
    file_seq_id     TEXT NOT NULL,
    batch_no        INT  NOT NULL,                -- 1-based
    record_count    INT  NOT NULL,
    first_row       BIGINT NOT NULL,              -- source row number of first record
    payload         JSONB NOT NULL,               -- {"records":[...], "meta":{...}}
    created_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE (file_seq_id, batch_no)                -- re-delivery safe
);
CREATE INDEX IF NOT EXISTS ix_batches_job ON parsed_batches(job_id);
