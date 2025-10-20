{{--
Add these to your `sources.yml` file to use source references dynamically:

sources:
  - name: semantic_layer_demo
    tables:
      - name: dim_customers
      - name: fct_orders
      - name: order_items
--}}

{{ config(materialized='table') }}

WITH combined_orders AS (
  SELECT
    c.name,
    oi.is_food_item,
    oi.is_drink_item,
    1 AS order_item_count -- Use 1 to count later for performance optimization
  FROM
    {{ source('semantic_layer_demo', 'dim_customers') }} c
    JOIN {{ source('semantic_layer_demo', 'fct_orders') }} o ON c.customer_id = o.customer_id
    JOIN {{ source('semantic_layer_demo', 'order_items') }} oi ON o.order_id = oi.order_id
  WHERE
    oi.is_food_item = 1 OR oi.is_drink_item = 1
),

food_orders AS (
  SELECT
    name,
    COUNT(order_item_count) AS item_count -- Count 1 per row instead of counting join keys
  FROM
    combined_orders
  WHERE
    is_food_item = 1
  GROUP BY
    name
),

drink_orders AS (
  SELECT
    name,
    COUNT(order_item_count) AS item_count
  FROM
    combined_orders
  WHERE
    is_drink_item = 1
  GROUP BY
    name
)

SELECT
  name,
  item_count,
  'food' AS item_type
FROM
  food_orders

UNION ALL -- UNION ALL is preferred over UNION for performance if duplicates are not expected

SELECT
  name,
  item_count,
  'drink' AS item_type
FROM
  drink_orders;