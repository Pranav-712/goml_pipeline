-- Query 5 (extra): top 10 entities by USD spend with their share of total USD spend and the
-- cumulative share (Pareto view). RANK() + running SUM() window functions.
WITH entity_spend AS (
    SELECT e.entity_id, e.first_name, e.last_name, e.region,
           SUM(f.transaction_amount) AS total_spend_usd,
           COUNT(*)                  AS transaction_count
    FROM fact_transactions f
    JOIN dim_entity e ON e.entity_key = f.entity_key
    WHERE f.currency = 'USD'
    GROUP BY e.entity_id, e.first_name, e.last_name, e.region
),
ranked AS (
    SELECT *,
           RANK() OVER (ORDER BY total_spend_usd DESC)                         AS spend_rank,
           ROUND(100.0 * total_spend_usd / SUM(total_spend_usd) OVER (), 2)    AS pct_of_total_usd,
           ROUND(100.0 * SUM(total_spend_usd) OVER (ORDER BY total_spend_usd DESC, entity_id
                                                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                 / SUM(total_spend_usd) OVER (), 2)                            AS cumulative_pct_usd
    FROM entity_spend
)
SELECT spend_rank, entity_id, first_name, last_name, region, total_spend_usd,
       transaction_count, pct_of_total_usd, cumulative_pct_usd
FROM ranked
WHERE spend_rank <= 10
ORDER BY spend_rank, entity_id;
