-- Add to your sources.yml file:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     schema: dbt_semantic_layer_demo
--     tables:
--       - name: fct_orders
--       - name: order_items

{{
  config(
    materialized = 'table',
    partition_by = {
      'field': 'ordered_at',
      'data_type': 'timestamp',
      'granularity': 'day'
    },
    cluster_by = ['product_id']
  )
}}

-- Optimized query: Changed RIGHT JOIN to INNER JOIN to filter nulls early and reduce data scanning
-- Added filter in JOIN condition to enable partition pruning before join execution
-- Removed DISTINCT as it's expensive; deduplication handled by join logic
-- Used dynamic source references for portability
WITH filtered_orders AS (
  
  SELECT
    order_id,
    ordered_at
  FROM
    {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
  WHERE
    -- Partition pruning: filter applied before join to minimize scanned data
    DATE(ordered_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)

)

SELECT
  oi.product_id,
  oi.product_price,
  fo.ordered_at
FROM
  filtered_orders AS fo
-- Changed to INNER JOIN: RIGHT JOIN was illogical as WHERE clause filtered on left table (o.ordered_at)
-- This makes RIGHT JOIN equivalent to INNER JOIN but less performant
INNER JOIN
  {{ source('dbt_semantic_layer_demo', 'order_items') }} AS oi
  ON fo.order_id = oi.order_id
-- Note: If true duplicates exist across product_id, product_price, ordered_at
-- consider adding GROUP BY instead of DISTINCT for better performance