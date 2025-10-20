{{
  config(
    materialized='table'
  )
}}

-- Add to your sources.yml:
-- sources:
--   - name: dbt_semantic_layer_demo
--     tables:
--       - name: fct_orders
--       - name: order_items

SELECT
  oi.product_id,
  oi.product_price,
  o.ordered_at
FROM
  {{ source('dbt_semantic_layer_demo', 'order_items') }} oi
JOIN
  {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
    ON o.order_id = oi.order_id
WHERE
  o.ordered_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
-- Removed DISTINCT by switching RIGHT JOIN to JOIN and filtering on non-null ordered_at
-- Filter applied directly on o.ordered_at without wrapping on DATE() for performance (assuming ordered_at is DATE/TIMESTAMP)