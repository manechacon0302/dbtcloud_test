-- Add these source definitions to your sources.yml file:
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
    partition_by=null,
    cluster_by=['name', 'item_type']
  )
}}

-- Optimized by consolidating CTEs into a single scan of the tables with UNION ALL
-- This reduces the number of joins from 6 to 3, improving performance and reducing costs
WITH customer_item_counts AS (
  SELECT
    c.name,
    -- Use COUNTIF to aggregate conditionally in a single pass instead of multiple CTEs
    COUNTIF(oi.is_food_item = 1) AS food_item_count,
    COUNTIF(oi.is_drink_item = 1) AS drink_item_count
  FROM
    {{ source('dbt_semantic_layer_demo', 'dim_customers') }} AS c
    INNER JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} AS o 
      ON c.customer_id = o.customer_id
    INNER JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} AS oi 
      ON o.order_id = oi.order_id
  WHERE
    -- Pre-filter to reduce data processed during joins
    (oi.is_food_item = 1 OR oi.is_drink_item = 1)
  GROUP BY
    c.name
)

-- Unpivot the results to maintain the original output structure
SELECT
  name,
  food_item_count AS item_count,
  'food' AS item_type
FROM
  customer_item_counts
WHERE
  food_item_count > 0

UNION ALL

SELECT
  name,
  drink_item_count AS item_count,
  'drink' AS item_type
FROM
  customer_item_counts
WHERE
  drink_item_count > 0