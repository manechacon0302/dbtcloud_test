-- Add these source definitions to your sources.yml file:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     schema: dbt_semantic_layer_demo
--     tables:
--       - name: fct_orders
--       - name: order_items

{{
  config(
    materialized='table',
    partition_by={
      "field": "ordered_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=["product_id"]
  )
}}

-- Optimized query: Changed RIGHT JOIN to INNER JOIN since WHERE clause filters on o.ordered_at (non-nullable after filter)
-- This eliminates rows where o.order_id would be NULL, making RIGHT JOIN equivalent to INNER JOIN
-- INNER JOIN is more performant and clearer in intent
SELECT DISTINCT
  oi.product_id,
  oi.product_price,
  o.ordered_at
FROM {{ source('dbt_semantic_layer_demo', 'order_items') }} AS oi
INNER JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} AS o
  ON oi.order_id = o.order_id
WHERE
  -- Partition filter applied for query cost optimization (scans only last 90 days)
  DATE(o.ordered_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)