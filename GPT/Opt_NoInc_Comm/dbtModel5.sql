-- Add the following source configurations in your sources.yml file:
-- 
-- sources:
--   - name: semantic_layer_demo
--     tables:
--       - name: dim_customers
--       - name: fct_orders
--       - name: order_items

{{ 
  config(
    materialized='table'
  ) 
}}

WITH high_value_customers AS (
    -- Use aggregation in a CTE to filter customers with total orders > 500 efficiently
    SELECT 
        customer_id
    FROM {{ source('semantic_layer_demo', 'fct_orders') }}
    GROUP BY customer_id
    HAVING SUM(order_total) > 500
),

-- Fix join condition for order_items - join on order_id instead of customer_id and avoid joining dim_customers twice unnecessarily
orders_with_items_filtered AS (
    SELECT 
        o.customer_id,
        oi.product_id,
        oi.product_price
    FROM {{ source('semantic_layer_demo', 'fct_orders') }} o
    JOIN {{ source('semantic_layer_demo', 'order_items') }} oi 
      ON o.order_id = oi.order_id  -- corrected join condition here
    WHERE oi.product_price > 20
)

SELECT 
    c.name,
    owi.product_id,
    owi.product_price
FROM {{ source('semantic_layer_demo', 'dim_customers') }} c
JOIN orders_with_items_filtered owi
  ON c.customer_id = owi.customer_id
JOIN high_value_customers hvc
  ON c.customer_id = hvc.customer_id

-- OPTIMIZATIONS APPLIED:
-- 1. Replaced hard-coded table references with dbt source() functions for maintainability and environment portability.
-- 2. Corrected join condition between order_items and fct_orders to join on order_id instead of customer_id.
-- 3. Pre-aggregated customers with sum(order_total) > 500 in a CTE for efficient filtering and reduced repeated subquery execution.
-- 4. Filtered product_price > 20 early to reduce data volume before the final join.
-- 5. Eliminated redundant joins and used clear aliases.
-- 6. Added comments to clarify logic and optimizations.
-- 7. Set materialization to 'table' (you can adjust as needed).