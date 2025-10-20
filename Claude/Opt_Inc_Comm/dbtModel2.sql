```
-- Add to your sources.yml file:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     schema: dbt_semantic_layer_demo
--     tables:
--       - name: dim_customers
--       - name: fct_orders

{{ config(
    materialized='table'
) }}

-- Optimized query replacing correlated subquery with JOIN for better performance
-- Removed unnecessary subquery wrapper around fct_orders
-- JOIN operation is more efficient in BigQuery than correlated subqueries
SELECT
    c.name AS customer_name,
    SUM(o.order_total) AS total_spent
FROM
    {{ source('dbt_semantic_layer_demo', 'fct_orders') }} AS o
    -- Using INNER JOIN instead of correlated subquery reduces query execution time
    INNER JOIN {{ source('dbt_semantic_layer_demo', 'dim_customers') }} AS c
        ON c.customer_id = o.customer_id
GROUP BY
    c.name
ORDER BY
    total_spent DESC
```