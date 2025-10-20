-- Add these source definitions to your schema.yml file:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     tables:
--       - name: dim_customers
--       - name: fct_orders
--       - name: order_items

{{ config(
    materialized='table',
    partition_by={
      "field": "order_date",
      "data_type": "date"
    },
    cluster_by=['customer_id', 'product_id']
) }}

-- Optimized query with CTE to avoid subquery recomputation and corrected join logic
WITH high_value_customers AS (
    -- Pre-filter customers with total orders > 500 to reduce join dataset
    SELECT 
        customer_id
    FROM {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
    GROUP BY customer_id
    HAVING SUM(order_total) > 500
),

filtered_order_items AS (
    -- Pre-filter order items by price threshold to reduce join dataset
    SELECT 
        order_id,
        product_id,
        product_price
    FROM {{ source('dbt_semantic_layer_demo', 'order_items') }}
    WHERE product_price > 20
)

SELECT 
    c.name,
    c.customer_id,  -- Added missing comma in original SELECT and including customer_id for clustering
    oi.product_id,
    oi.product_price,
    o.order_date  -- Added for partitioning capability
FROM {{ source('dbt_semantic_layer_demo', 'dim_customers') }} AS c
-- Join with high value customers first to reduce dataset early
INNER JOIN high_value_customers AS hvc
    ON c.customer_id = hvc.customer_id
-- Join with orders using correct join key
INNER JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} AS o
    ON c.customer_id = o.customer_id
-- Fixed join condition: order_items should join on order_id, not customer_id
INNER JOIN filtered_order_items AS oi
    ON o.order_id = oi.order_id