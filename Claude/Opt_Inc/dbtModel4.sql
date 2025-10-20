-- Add to sources.yml:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     tables:
--       - name: dim_customers
--       - name: fct_orders
--       - name: order_items

{{ config(
    materialized='table'
) }}

WITH base_data AS (
  SELECT
    c.name,
    oi.order_item_id,
    oi.is_food_item,
    oi.is_drink_item
  FROM
    {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
    JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o 
      ON c.customer_id = o.customer_id
    JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} oi 
      ON o.order_id = oi.order_id
  WHERE
    (oi.is_food_item = 1 OR oi.is_drink_item = 1)
),

aggregated_data AS (
  SELECT
    name,
    COUNTIF(is_food_item = 1) AS food_item_count,
    COUNTIF(is_drink_item = 1) AS drink_item_count
  FROM
    base_data
  GROUP BY
    name
)

SELECT
  name,
  food_item_count AS item_count,
  'food' AS item_type
FROM
  aggregated_data
WHERE
  food_item_count > 0

UNION ALL

SELECT
  name,
  drink_item_count AS item_count,
  'drink' AS item_type
FROM
  aggregated_data
WHERE
  drink_item_count > 0