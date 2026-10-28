-- Silver: cleaned, de-duplicated, standardised and conformed. INCREMENTAL / idempotent.
-- Reads only the Bronze rows of the batches listed in the temp table _new_batches
-- (all batches on a full refresh), and MERGES them into the persistent Silver tables.
--
-- De-duplication rule: the first-received record wins (ordered by source file, then row);
-- a key that already exists in Silver is never overwritten (ON CONFLICT DO NOTHING).

CREATE TABLE IF NOT EXISTS silver_entities (
    entity_id         UUID PRIMARY KEY,
    first_name        VARCHAR,
    last_name         VARCHAR,
    contact_email     VARCHAR,
    registration_date TIMESTAMP,
    region            VARCHAR
);

CREATE TABLE IF NOT EXISTS silver_transactions (
    transaction_id     UUID PRIMARY KEY,
    entity_id          UUID NOT NULL,
    category           VARCHAR,
    transaction_amount DECIMAL(18, 2),
    currency           VARCHAR,
    "timestamp"        TIMESTAMP,
    transaction_date   DATE
);

-- Quarantine (not silently dropped): valid payloads whose entity is not (yet) known.
CREATE TABLE IF NOT EXISTS silver_orphan_transactions (
    transaction_id     UUID PRIMARY KEY,
    entity_id          UUID NOT NULL,
    category           VARCHAR,
    transaction_amount DECIMAL(18, 2),
    currency           VARCHAR,
    "timestamp"        TIMESTAMP,
    transaction_date   DATE
);

-- ---- entities: ROW_NUMBER() de-duplication + standardisation ----------------------------
CREATE OR REPLACE TEMP TABLE _entities_new AS
SELECT
    r.entity_id,
    TRIM(r.first_name)                                        AS first_name,
    TRIM(r.last_name)                                         AS last_name,
    LOWER(TRIM(r.contact_email))                              AS contact_email,
    r.registration_date,
    UPPER(REGEXP_REPLACE(TRIM(r.region), '\s+', ' ', 'g'))    AS region   -- north / " South " -> NORTH / SOUTH
FROM (
    SELECT
        b.*,
        ROW_NUMBER() OVER (PARTITION BY b.entity_id ORDER BY b._source_file, b._source_row) AS rn
    FROM bronze_entities b
    JOIN _new_batches n ON n._batch_id = b._batch_id
) r
WHERE r.rn = 1;

INSERT INTO silver_entities
SELECT * FROM _entities_new WHERE true
ON CONFLICT DO NOTHING;

-- ---- transactions: de-duplicate on transaction_id, standardise, derive the date ----------
CREATE OR REPLACE TEMP TABLE _tx_new AS
SELECT
    r.transaction_id,
    r.entity_id,
    TRIM(r.category)                                         AS category,
    CAST(ROUND(r.transaction_amount, 2) AS DECIMAL(18, 2))   AS transaction_amount,
    UPPER(TRIM(r.currency))                                  AS currency,
    CAST(r."timestamp" AS TIMESTAMP)                         AS "timestamp",
    CAST(r."timestamp" AS DATE)                              AS transaction_date
FROM (
    SELECT
        b.*,
        ROW_NUMBER() OVER (PARTITION BY b.transaction_id ORDER BY b._source_file, b._source_row) AS rn
    FROM bronze_transactions b
    JOIN _new_batches n ON n._batch_id = b._batch_id
) r
WHERE r.rn = 1
  AND NOT EXISTS (SELECT 1 FROM silver_transactions s        WHERE s.transaction_id = r.transaction_id)
  AND NOT EXISTS (SELECT 1 FROM silver_orphan_transactions o WHERE o.transaction_id = r.transaction_id);

-- referential check: conform only transactions whose entity exists
INSERT INTO silver_transactions
SELECT t.* FROM _tx_new t
WHERE EXISTS (SELECT 1 FROM silver_entities e WHERE e.entity_id = t.entity_id);

INSERT INTO silver_orphan_transactions
SELECT t.* FROM _tx_new t
WHERE NOT EXISTS (SELECT 1 FROM silver_entities e WHERE e.entity_id = t.entity_id);

-- late-arriving entities: re-home earlier orphans whose entity has now been loaded
INSERT INTO silver_transactions
SELECT o.* FROM silver_orphan_transactions o
WHERE EXISTS (SELECT 1 FROM silver_entities e WHERE e.entity_id = o.entity_id);

DELETE FROM silver_orphan_transactions
WHERE transaction_id IN (SELECT transaction_id FROM silver_transactions);
