-- Add these source configurations to your sources.yml file:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     tables:
--       - name: dim_customers
--       - name: fct_orders
--       - name: order_items

{{ config(
    materialized='table',
    labels={'optimization': 'finops', 'type': 'analytical'}
) }}

WITH high_value_customers AS (
    -- Pre-filter customers with total orders > 500 to reduce join size
    -- Using CTE instead of correlated subquery for better performance
    SELECT 
        customer_id
    FROM {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
    GROUP BY customer_id
    HAVING SUM(order_total) > 500
),

filtered_order_items AS (
    -- Apply filter early to reduce data volume before joins
    SELECT 
        order_id,
        product_id,
        product_price
    FROM {{ source('dbt_semantic_layer_demo', 'order_items') }}
    WHERE product_price > 20
)

-- Main query with optimized join order: smallest table first
SELECT 
    c.name,
    oi.product_id,
    oi.product_price
FROM high_value_customers hvc
INNER JOIN {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
    ON hvc.customer_id = c.customer_id
INNER JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
    ON c.customer_id = o.customer_id
INNER JOIN filtered_order_items oi
    -- Fixed join condition: should be o.order_id = oi.order_id (not c.customer_id = oi.order_id)
    ON o.order_id = oi.order_id