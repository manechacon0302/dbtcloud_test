-- Add these sources to your sources.yml file:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     tables:
--       - name: fct_orders
--       - name: order_items

{{ config(
    materialized='table',
    partition_by={
      "field": "order_month_date",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by=['rank', 'product_id']
) }}

WITH base_order_items AS (
    -- Pre-aggregate at product-month level to reduce data processed in window function
    SELECT
        FORMAT_TIMESTAMP('%Y-%m', o.ordered_at) AS order_month,
        DATE_TRUNC(DATE(o.ordered_at), MONTH) AS order_month_date, -- Added for partitioning
        oi.product_id,
        SUM(oi.product_price) AS total_revenue
    FROM
        {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
    INNER JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} oi -- Changed from implicit to explicit JOIN for better performance
        ON o.order_id = oi.order_id
    GROUP BY
        1, 2, 3 -- Fixed GROUP BY to include all non-aggregated columns
)

SELECT
    order_month,
    order_month_date, -- Kept for partitioning purposes
    product_id,
    total_revenue,
    RANK() OVER (
        PARTITION BY order_month 
        ORDER BY total_revenue DESC
    ) AS rank -- Window function now operates on pre-aggregated data
FROM
    base_order_items