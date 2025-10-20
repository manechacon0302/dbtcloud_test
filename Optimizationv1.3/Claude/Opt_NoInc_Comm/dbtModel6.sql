-- Add these source definitions to your sources.yml file:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     tables:
--       - name: fct_orders
--       - name: order_items

{{ config(
    materialized='table',
    labels={'optimization': 'finops'}
) }}

SELECT
    FORMAT_TIMESTAMP('%Y-%m', o.ordered_at) AS order_month,
    oi.product_id,
    SUM(oi.product_price) AS total_revenue,
    -- Using RANK() with window function - optimized by calculating aggregate once in GROUP BY
    RANK() OVER (
        PARTITION BY FORMAT_TIMESTAMP('%Y-%m', o.ordered_at) 
        ORDER BY SUM(oi.product_price) DESC
    ) AS rank
FROM
    {{ source('dbt_semantic_layer_demo', 'fct_orders') }} AS o
    -- Replaced implicit comma join with explicit INNER JOIN for better query optimization
    INNER JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} AS oi
        ON o.order_id = oi.order_id
-- Added product_id to GROUP BY to fix aggregation error and match SELECT clause
GROUP BY 
    order_month,
    oi.product_id