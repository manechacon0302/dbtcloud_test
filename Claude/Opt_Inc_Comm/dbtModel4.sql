-- Add to your sources.yml file:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     schema: dbt_semantic_layer_demo
--     tables:
--       - name: dim_customers
--       - name: fct_orders
--       - name: order_items

{{ config(
    materialized='table',
    description='Customer order counts by item type (food/drink)'
) }}

-- Optimized query using UNION ALL instead of UNION to avoid expensive deduplication
-- Consolidated joins to eliminate redundant scans of the same tables
-- Used COUNTIF for conditional aggregation instead of separate CTEs and UNION
WITH customer_order_items AS (
  -- Single pass through all tables to reduce I/O and compute costs
  SELECT
    c.name,
    oi.is_food_item,
    oi.is_drink_item,
    oi.order_item_id
  FROM
    {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
    JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o 
      ON c.customer_id = o.customer_id
    JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} oi 
      ON o.order_id = oi.order_id
  WHERE
    -- Filter early to reduce data volume in aggregation
    (oi.is_food_item = 1 OR oi.is_drink_item = 1)
),

aggregated_items AS (
  -- Aggregate once instead of twice with separate CTEs
  SELECT
    name,
    COUNTIF(is_food_item = 1) AS food_count,
    COUNTIF(is_drink_item = 1) AS drink_count
  FROM
    customer_order_items
  GROUP BY
    name
)

-- Unpivot the aggregated results to match original output structure
SELECT
  name,
  food_count AS item_count,
  'food' AS item_type
FROM
  aggregated_items
WHERE
  food_count > 0

UNION ALL

SELECT
  name,
  drink_count AS item_count,
  'drink' AS item_type
FROM
  aggregated_items
WHERE
  drink_count > 0