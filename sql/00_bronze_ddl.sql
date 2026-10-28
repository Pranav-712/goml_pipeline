-- Bronze + batch ledger. All statements are idempotent (CREATE ... IF NOT EXISTS) so the same
-- script serves both full refresh (after a reset) and incremental runs.
--
-- Bronze keeps contract-valid records exactly as received (duplicates and messy casing
-- included) plus lineage columns: which file / row / batch a record came from.

-- Ledger of landed files. status: LOADED (in Bronze) -> COMPLETE (Silver + Gold done).
-- A batch left in LOADED (e.g. the run crashed during transform) is picked up by the next run.
CREATE TABLE IF NOT EXISTS etl_batches (
    batch_id     VARCHAR PRIMARY KEY,   -- first 16 hex chars of the file's SHA-256
    source       VARCHAR NOT NULL,      -- 'entities' | 'transactions'
    source_file  VARCHAR NOT NULL,
    file_sha256  VARCHAR NOT NULL,
    rows_read    INTEGER,
    rows_valid   INTEGER,
    rows_invalid INTEGER,
    status       VARCHAR NOT NULL,
    loaded_at    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze_entities (
    entity_id         UUID,
    first_name        VARCHAR,
    last_name         VARCHAR,
    contact_email     VARCHAR,
    registration_date TIMESTAMP,
    region            VARCHAR,
    _source_file      VARCHAR,
    _source_row       INTEGER,    -- 1-based record number within the source file
    _batch_id         VARCHAR,
    _ingested_at      TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze_transactions (
    transaction_id     UUID,
    entity_id          UUID,
    category           VARCHAR,
    transaction_amount DOUBLE,
    currency           VARCHAR,
    "timestamp"        TIMESTAMP,
    _source_file       VARCHAR,
    _source_row        INTEGER,
    _batch_id          VARCHAR,
    _ingested_at       TIMESTAMP
);
