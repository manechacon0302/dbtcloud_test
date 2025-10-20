-- Add these sources to your sources.yml file:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     schema: dbt_semantic_layer_demo
--     tables:
--       - name: dim_customers
--       - name: fct_orders
--       - name: order_items

{{
  config(
    materialized='table',
    labels={'domain': 'orders', 'type': 'customer_items'}
  )
}}

-- Optimized: Eliminated duplicate joins and scans by using conditional aggregation instead of UNION
-- Optimized: Replaced UNION with UNION ALL pattern through UNNEST for better performance
-- Optimized: Reduced the number of table scans from 6 to 3 by processing both item types in a single pass
WITH customer_item_counts AS (
  SELECT
    c.name,
    -- Conditional aggregation to count food and drink items in a single pass
    COUNTIF(oi.is_food_item = 1) AS food_item_count,
    COUNTIF(oi.is_drink_item = 1) AS drink_item_count
  FROM
    {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
    JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o 
      ON c.customer_id = o.customer_id
    JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} oi 
      ON o.order_id = oi.order_id
  WHERE
    -- Filter early to reduce rows processed in aggregation
    (oi.is_food_item = 1 OR oi.is_drink_item = 1)
  GROUP BY
    c.name
)

-- Unpivot the aggregated results to match the original output structure
SELECT
  name,
  item_count,
  item_type
FROM
  customer_item_counts,
  UNNEST([
    STRUCT(food_item_count AS item_count, 'food' AS item_type),
    STRUCT(drink_item_count AS item_count, 'drink' AS item_type)
  ])
WHERE
  -- Filter out rows with zero counts to match original UNION behavior
  item_count > 0