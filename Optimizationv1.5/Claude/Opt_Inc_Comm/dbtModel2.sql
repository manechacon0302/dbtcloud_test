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
    labels={'data_source': 'semantic_layer_demo'}
) }}

-- Optimized: Replaced correlated subquery with JOIN for better performance
-- Optimized: Removed unnecessary subquery wrapper around fct_orders
-- Optimized: JOIN is more efficient than correlated subquery as it avoids row-by-row execution
WITH orders_base AS (
  SELECT
    customer_id,
    order_total
  FROM
    {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
),

customers_base AS (
  SELECT
    customer_id,
    name
  FROM
    {{ source('dbt_semantic_layer_demo', 'dim_customers') }}
)

-- Main query with optimized JOIN instead of correlated subquery
SELECT
  c.name AS customer_name,
  SUM(o.order_total) AS total_spent
FROM
  orders_base o
INNER JOIN
  customers_base c
  ON o.customer_id = c.customer_id
GROUP BY
  c.name
ORDER BY
  total_spent DESC