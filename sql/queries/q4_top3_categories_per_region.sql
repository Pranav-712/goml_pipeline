-- Query 4 (extra): top 3 categories per region by number of transactions.
-- Counts (not amounts) so the ranking is unaffected by mixed currencies.
WITH region_category AS (
    SELECT e.region, f.category, COUNT(*) AS transaction_count
    FROM fact_transactions f
    JOIN dim_entity e ON e.entity_key = f.entity_key
    GROUP BY e.region, f.category
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY transaction_count DESC, category) AS category_rank
    FROM region_category
)
SELECT region, category_rank, category, transaction_count
FROM ranked
WHERE category_rank <= 3
ORDER BY region, category_rank;
