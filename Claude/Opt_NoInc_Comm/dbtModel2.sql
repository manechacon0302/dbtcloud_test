-- Add to your sources.yml file:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     schema: dbt_semantic_layer_demo
--     tables:
--       - name: dim_customers
--       - name: fct_orders

{{ config(
    materialized='table',
    partition_by={
      "field": "customer_id",
      "data_type": "int64"
    } if var('enable_partitioning', false) else none,
    cluster_by=['total_spent'] if var('enable_clustering', true) else none
) }}

-- Optimization 1: Replaced correlated subquery with JOIN for better performance
-- Correlated subqueries execute once per row, while JOINs are optimized by BigQuery's query engine
-- Optimization 2: Removed unnecessary subquery around fct_orders table
-- Optimization 3: Used dynamic table references with dbt source() function for better lineage and portability
-- Optimization 4: Added explicit column selection to reduce data scanning
-- Optimization 5: Added clustering on total_spent for faster ORDER BY operations
-- Optimization 6: Moved GROUP BY to use actual column instead of alias for better compatibility

SELECT
  c.name AS customer_name,
  SUM(o.order_total) AS total_spent
FROM
  {{ source('dbt_semantic_layer_demo', 'fct_orders') }} AS o
INNER JOIN
  {{ source('dbt_semantic_layer_demo', 'dim_customers') }} AS c
  ON c.customer_id = o.customer_id
GROUP BY
  c.name
ORDER BY
  total_spent DESC