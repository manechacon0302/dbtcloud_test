-- Add to your sources.yml file:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     tables:
--       - name: dim_customers
--       - name: fct_orders
--       - name: order_items

{{ config(
    materialized='table',
    labels={'optimization': 'finops'}
) }}

-- CTE to pre-filter high-value customers to avoid redundant subquery execution
WITH high_value_customers AS (
    SELECT 
        customer_id
    FROM {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
    GROUP BY customer_id
    HAVING SUM(order_total) > 500
),

-- Pre-filter order_items to reduce join volume
filtered_order_items AS (
    SELECT 
        order_id,
        product_id,
        product_price
    FROM {{ source('dbt_semantic_layer_demo', 'order_items') }}
    WHERE product_price > 20
)

-- Main query with optimized joins
SELECT 
    c.name,
    oi.product_id,
    oi.product_price
FROM {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
-- Filter customers early using INNER JOIN instead of IN clause for better performance
INNER JOIN high_value_customers hvc 
    ON c.customer_id = hvc.customer_id
-- Join with orders table
INNER JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o 
    ON c.customer_id = o.customer_id
-- Join with pre-filtered order items to reduce data volume
INNER JOIN filtered_order_items oi 
    ON o.order_id = oi.order_id