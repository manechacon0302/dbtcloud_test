-- Add these sources to your sources.yml file:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     schema: dbt_semantic_layer_demo
--     tables:
--       - name: dim_customers
--       - name: fct_orders
--       - name: order_items

{{ config(
    materialized='table',
    partition_by={
      "field": "customer_id",
      "data_type": "int64",
      "range": {
        "start": 0,
        "end": 100000000,
        "interval": 10000
      }
    },
    cluster_by=['customer_id']
) }}

-- OPTIMIZATIONS APPLIED:
-- 1. Fixed syntax error: missing comma after c.name
-- 2. Fixed logic error: incorrect join condition (c.customer_id = oi.order_id should be o.order_id = oi.order_id)
-- 3. Replaced hardcoded table references with dbt source references for maintainability
-- 4. Optimized subquery in WHERE clause by adding GROUP BY and moving to CTE for better query plan
-- 5. Changed correlated subquery to JOIN with aggregated CTE for better performance (avoids nested loop)
-- 6. Added clustering and partitioning by customer_id to improve query performance and reduce scan costs
-- 7. Removed redundant join to fct_orders in subquery by using the main query join
-- 8. Added proper indentation and formatting for readability

WITH high_value_customers AS (
    SELECT 
        customer_id
    FROM {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
    GROUP BY customer_id
    HAVING SUM(order_total) > 500
)

SELECT 
    c.name,
    oi.product_id,
    oi.product_price
FROM {{ source('dbt_semantic_layer_demo', 'dim_customers') }} AS c
INNER JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} AS o 
    ON c.customer_id = o.customer_id
INNER JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} AS oi 
    ON o.order_id = oi.order_id
INNER JOIN high_value_customers AS hvc 
    ON c.customer_id = hvc.customer_id
WHERE oi.product_price > 20