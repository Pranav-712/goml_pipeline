-- Query 2b (extra): Q2 computed separately per currency (no mixed-currency averages),
-- top 5 regions within each currency. Uses ROW_NUMBER() over a partitioned window.
WITH entity_avg AS (
    SELECT e.entity_key, e.region, f.currency, AVG(f.transaction_amount) AS entity_avg_amount
    FROM fact_transactions f
    JOIN dim_entity e ON e.entity_key = f.entity_key
    GROUP BY e.entity_key, e.region, f.currency
),
region_avg AS (
    SELECT region, currency,
           ROUND(AVG(entity_avg_amount), 2) AS average_transaction_amount_per_entity,
           COUNT(*) AS entity_count
    FROM entity_avg
    GROUP BY region, currency
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY currency
                                 ORDER BY average_transaction_amount_per_entity DESC, region) AS region_rank
    FROM region_avg
)
SELECT currency, region_rank, region, average_transaction_amount_per_entity, entity_count
FROM ranked
WHERE region_rank <= 5
ORDER BY currency, region_rank;
