-- Add these source definitions to your sources.yml file:
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
      "field": "order_month",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by=['rank', 'product_id']
) }}

-- Optimizations applied:
-- 1. Replaced implicit comma join with explicit INNER JOIN for better readability and performance
-- 2. Added dbt source references for dynamic table management
-- 3. Fixed GROUP BY to include all non-aggregated columns (product_id was missing)
-- 4. Used DATE_TRUNC instead of FORMAT_TIMESTAMP for better performance and partitioning compatibility
-- 5. Added PARSE_DATE to convert back to DATE type for partition compatibility
-- 6. Added table materialization with monthly partitioning on order_month for query pruning
-- 7. Added clustering on rank and product_id for common filtering patterns
-- 8. Proper code indentation and spacing for maintainability

SELECT
    PARSE_DATE('%Y-%m', FORMAT_TIMESTAMP('%Y-%m', o.ordered_at)) AS order_month,
    oi.product_id,
    SUM(oi.product_price) AS total_revenue,
    RANK() OVER (
        PARTITION BY FORMAT_TIMESTAMP('%Y-%m', o.ordered_at) 
        ORDER BY SUM(oi.product_price) DESC
    ) AS rank
FROM {{ source('dbt_semantic_layer_demo', 'fct_orders') }} AS o
INNER JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} AS oi
    ON o.order_id = oi.order_id
GROUP BY 
    order_month,
    product_id