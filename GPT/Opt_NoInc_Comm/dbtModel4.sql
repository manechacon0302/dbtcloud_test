{{  
  /*
  Add to your sources.yml:

  version: 2

  sources:
    - name: dbt_semantic_layer_demo
      database: propellingtech-demo-customers
      schema: dbt_semantic_layer_demo
      tables:
        - name: dim_customers
        - name: fct_orders
        - name: order_items
  */
}}

{{ config(
    materialized='table'  -- materialized as table for performance
) }}

WITH base_data AS (
  SELECT
    c.name,
    oi.order_item_id,
    oi.is_food_item,
    oi.is_drink_item
  FROM {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
  JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
    ON c.customer_id = o.customer_id
  JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} oi
    ON o.order_id = oi.order_id
  WHERE oi.is_food_item = 1 OR oi.is_drink_item = 1
  -- filtered only relevant rows early for performance
)

SELECT 
  name,
  COUNTIF(is_food_item = 1) AS item_count,
  'food' AS item_type
FROM base_data
WHERE is_food_item = 1
GROUP BY name

UNION ALL

SELECT 
  name,
  COUNTIF(is_drink_item = 1) AS item_count,
  'drink' AS item_type
FROM base_data
WHERE is_drink_item = 1
GROUP BY name

-- Used UNION ALL as the queries are mutually exclusive (food or drink) to avoid unnecessary deduplication
-- Replaced repeated joins with a single base_data CTE
-- Applied early filter on is_food_item or is_drink_item to reduce data scanned
-- Used COUNTIF aggregation for concise counting per item type