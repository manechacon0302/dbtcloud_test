-- Add to your sources.yml:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     tables:
--       - name: dim_customers
--       - name: fct_orders
--       - name: order_items

{{ config(
    materialized='table',
    partition_by=null,
    cluster_by=['customer_id', 'product_id']
) }}

-- Optimized query with CTE for better readability and performance
-- Using CTE to pre-filter high-value customers and avoid correlated subquery
WITH high_value_customers AS (
    -- Pre-aggregate to identify customers with orders > 500
    SELECT 
        customer_id
    FROM {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
    GROUP BY customer_id
    HAVING SUM(order_total) > 500
),

filtered_order_items AS (
    -- Pre-filter order items by price threshold to reduce join volume
    SELECT 
        order_id,
        product_id,
        product_price
    FROM {{ source('dbt_semantic_layer_demo', 'order_items') }}
    WHERE product_price > 20
)

-- Main query with optimized join order: dimension -> fact -> filtered items
SELECT 
    c.name,
    oi.product_id,
    oi.product_price
FROM {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
-- Join with pre-filtered high-value customers first to reduce dataset early
INNER JOIN high_value_customers hvc
    ON c.customer_id = hvc.customer_id
-- Join with fact table
INNER JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
    ON c.customer_id = o.customer_id
-- Join with pre-filtered order items
INNER JOIN filtered_order_items oi
    ON o.order_id = oi.order_id