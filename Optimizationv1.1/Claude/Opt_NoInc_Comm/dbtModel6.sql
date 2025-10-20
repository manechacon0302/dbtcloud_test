-- Add these lines to your sources.yml file:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     schema: dbt_semantic_layer_demo
--     tables:
--       - name: fct_orders
--       - name: order_items

{{ config(
    materialized='table',
    partition_by={
        'field': 'order_month',
        'data_type': 'date',
        'granularity': 'month'
    },
    cluster_by=['product_id']
) }}

WITH base_orders AS (
    -- Pre-aggregate at the order level to reduce JOIN cardinality
    SELECT
        DATE_TRUNC(ordered_at, MONTH) AS order_month,
        order_id
    FROM {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
    WHERE ordered_at IS NOT NULL  -- Filter out nulls before JOIN for better performance
),

order_items_agg AS (
    -- Aggregate order items before JOIN to reduce data volume
    SELECT
        order_id,
        product_id,
        SUM(product_price) AS total_revenue
    FROM {{ source('dbt_semantic_layer_demo', 'order_items') }}
    WHERE product_price IS NOT NULL  -- Filter out nulls early
    GROUP BY order_id, product_id
),

joined_data AS (
    -- Use explicit INNER JOIN syntax instead of implicit comma JOIN for better readability and optimization
    SELECT
        o.order_month,
        oi.product_id,
        oi.total_revenue
    FROM base_orders o
    INNER JOIN order_items_agg oi
        ON o.order_id = oi.order_id
),

monthly_product_revenue AS (
    -- Aggregate to monthly product level
    SELECT
        order_month,
        product_id,
        SUM(total_revenue) AS total_revenue
    FROM joined_data
    GROUP BY order_month, product_id
)

-- Calculate rank with window function
SELECT
    order_month,
    product_id,
    total_revenue,
    RANK() OVER (
        PARTITION BY order_month 
        ORDER BY total_revenue DESC
    ) AS rank
FROM monthly_product_revenue