```
-- Add to sources.yml:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     schema: dbt_semantic_layer_demo
--     tables:
--       - name: dim_customers
--       - name: fct_orders

{{ config(
    materialized='table',
    labels={'optimization': 'finops', 'model_type': 'analytics'}
) }}

-- Optimized query: Replaced correlated subquery with JOIN to avoid N+1 query execution
-- Removed unnecessary nested SELECT on fact table for better query plan
-- Added COALESCE for null safety on customer_name to ensure proper grouping
SELECT
  COALESCE(c.name, 'Unknown') AS customer_name,
  SUM(o.order_total) AS total_spent
FROM
  {{ source('dbt_semantic_layer_demo', 'fct_orders') }} AS o
  -- LEFT JOIN ensures all orders are included even if customer data is missing
  LEFT JOIN {{ source('dbt_semantic_layer_demo', 'dim_customers') }} AS c
    ON o.customer_id = c.customer_id
GROUP BY
  c.name -- Group by the actual column instead of alias for better BigQuery optimization
ORDER BY
  total_spent DESC
```