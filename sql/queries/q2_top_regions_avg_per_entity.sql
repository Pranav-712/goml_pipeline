-- Query 2: top 5 regions by average (entity-level) transaction amount.
-- Step 1: average per entity.  Step 2: average of those entity averages per region.
-- (Averaging per entity first stops very active entities dominating a region.)
WITH entity_avg AS (
    SELECT
        e.entity_key,
        e.region,
        AVG(f.transaction_amount) AS entity_avg_amount
    FROM fact_transactions f
    JOIN dim_entity e ON e.entity_key = f.entity_key
    GROUP BY e.entity_key, e.region
)
SELECT
    region,
    ROUND(AVG(entity_avg_amount), 2) AS average_transaction_amount_per_entity,
    COUNT(*)                         AS entity_count
FROM entity_avg
GROUP BY region
ORDER BY average_transaction_amount_per_entity DESC, region
LIMIT 5;
