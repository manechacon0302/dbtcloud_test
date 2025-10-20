-- Add to your sources.yml:
-- sources:
--   - name: semantic_layer_demo
--     tables:
--       - name: dim_customers
--       - name: fct_orders

{{ config(materialized='table') }}

WITH orders AS (
    SELECT
        customer_id,
        order_total
    FROM {{ source('semantic_layer_demo', 'fct_orders') }}
),

customers AS (
    SELECT
        customer_id,
        name AS customer_name
    FROM {{ source('semantic_layer_demo', 'dim_customers') }}
)

SELECT
    c.customer_name,
    SUM(o.order_total) AS total_spent
FROM orders o
JOIN customers c
  ON o.customer_id = c.customer_id
GROUP BY c.customer_name
ORDER BY total_spent DESC

-- Optimization notes:
-- 1. Replaced scalar subquery with a JOIN to avoid repeated lookups for each row (improves performance).
-- 2. Avoided SELECT * in subquery, selecting only needed columns to reduce data processing.
-- 3. Used dbt source() to make table references dynamic and maintainable.
-- 4. Removed unnecessary subquery wrapping around fct_orders table.
-- 5. Preserved original fields only (customer_name derived from dim_customers.name and total_spent).