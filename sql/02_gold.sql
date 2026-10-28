-- Gold: star schema. INCREMENTAL / idempotent: only rows not yet present are inserted.
-- Surrogate keys are stable across runs (new keys continue from MAX(entity_key)).

CREATE TABLE IF NOT EXISTS dim_entity (
    entity_key        INTEGER PRIMARY KEY,   -- surrogate key
    entity_id         UUID NOT NULL UNIQUE,  -- natural / business key
    first_name        VARCHAR,
    last_name         VARCHAR,
    contact_email     VARCHAR,
    registration_date TIMESTAMP,
    region            VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key   INTEGER PRIMARY KEY,          -- YYYYMMDD
    full_date  DATE NOT NULL UNIQUE,
    year       INTEGER,
    quarter    INTEGER,
    month      INTEGER,
    month_name VARCHAR,
    day        INTEGER
);

CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id     UUID PRIMARY KEY,
    entity_key         INTEGER NOT NULL REFERENCES dim_entity (entity_key),
    date_key           INTEGER NOT NULL REFERENCES dim_date (date_key),
    category           VARCHAR,
    transaction_amount DECIMAL(18, 2),
    currency           VARCHAR,
    "timestamp"        TIMESTAMP
);

INSERT INTO dim_entity
SELECT
    COALESCE((SELECT MAX(entity_key) FROM dim_entity), 0) + ROW_NUMBER() OVER (ORDER BY s.entity_id),
    s.entity_id, s.first_name, s.last_name, s.contact_email, s.registration_date, s.region
FROM silver_entities s
WHERE NOT EXISTS (SELECT 1 FROM dim_entity d WHERE d.entity_id = s.entity_id);

-- Contiguous calendar from the first to the last transaction date (no gaps), extended as needed.
-- A recursive CTE is used (rather than generate_series) because it behaves identically on every
-- DuckDB version >= 0.9.
CREATE OR REPLACE TEMP TABLE _days AS
WITH RECURSIVE bounds AS (
    SELECT MIN(transaction_date) AS lo, MAX(transaction_date) AS hi FROM silver_transactions
),
calendar(full_date) AS (
    SELECT lo FROM bounds WHERE lo IS NOT NULL
    UNION ALL
    SELECT CAST(c.full_date + 1 AS DATE) FROM calendar c, bounds b WHERE c.full_date < b.hi
)
SELECT full_date FROM calendar;

INSERT INTO dim_date
SELECT
    CAST(STRFTIME(full_date, '%Y%m%d') AS INTEGER) AS date_key,
    full_date,
    YEAR(full_date), QUARTER(full_date), MONTH(full_date), MONTHNAME(full_date), DAY(full_date)
FROM _days
WHERE NOT EXISTS (SELECT 1 FROM dim_date x WHERE x.full_date = _days.full_date);

INSERT INTO fact_transactions
SELECT
    t.transaction_id,
    e.entity_key,
    d.date_key,
    t.category,
    t.transaction_amount,
    t.currency,
    t."timestamp"
FROM silver_transactions t
JOIN dim_entity e ON e.entity_id = t.entity_id
JOIN dim_date   d ON d.full_date = t.transaction_date
WHERE NOT EXISTS (SELECT 1 FROM fact_transactions f WHERE f.transaction_id = t.transaction_id);
