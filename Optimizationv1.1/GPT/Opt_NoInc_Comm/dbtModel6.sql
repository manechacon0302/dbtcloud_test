-- Please add the following source definitions to your sources.yml file:
-- 
-- version: 2
-- sources:
--   - name: dbt_semantic_layer_demo
--     tables:
--       - name: fct_orders
--       - name: order_items

{{ config(
    materialized='table'
) }}

WITH base_data AS (
    SELECT
        -- Use DATE_TRUNC rather than FORMAT_TIMESTAMP for better performance and native DATE type handling
        DATE_TRUNC(DATE(o.ordered_at), MONTH) AS order_month,
        oi.product_id,
        oi.product_price
    FROM {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
    INNER JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} oi
      ON o.order_id = oi.order_id
),

aggregated AS (
    SELECT
        order_month,
        product_id,
        SUM(product_price) AS total_revenue
    FROM base_data
    GROUP BY order_month, product_id
)

SELECT
    order_month,
    product_id,
    total_revenue,
    -- Avoid repeated calculations by using aggregated total_revenue for ranking
    RANK() OVER (
        PARTITION BY order_month
        ORDER BY total_revenue DESC
    ) AS rank
FROM aggregated
ORDER BY order_month, rank;