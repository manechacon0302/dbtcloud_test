-- Add to your sources.yml file:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     tables:
--       - name: fct_orders
--       - name: order_items

{{
  config(
    materialized='incremental',
    unique_key=['order_month', 'product_id'],
    partition_by={
      'field': 'order_date',
      'data_type': 'date',
      'granularity': 'month'
    },
    cluster_by=['order_month', 'product_id'],
    on_schema_change='sync_all_columns'
  )
}}

WITH base_orders AS (
  -- Filter orders incrementally to reduce data scanned
  SELECT
    order_id,
    ordered_at,
    DATE(ordered_at) AS order_date
  FROM {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
  {% if is_incremental() %}
  -- Only process new or updated records since last run
  WHERE DATE(ordered_at) >= (SELECT DATE_SUB(MAX(order_date), INTERVAL 3 DAY) FROM {{ this }})
  {% endif %}
),

base_order_items AS (
  -- Pre-filter order_items based on incremental orders to reduce join size
  SELECT
    oi.order_id,
    oi.product_id,
    oi.product_price
  FROM {{ source('dbt_semantic_layer_demo', 'order_items') }} oi
  {% if is_incremental() %}
  WHERE oi.order_id IN (SELECT order_id FROM base_orders)
  {% endif %}
),

aggregated_revenue AS (
  -- Perform aggregation before window function for better performance
  SELECT
    FORMAT_TIMESTAMP('%Y-%m', o.ordered_at) AS order_month,
    o.order_date,
    oi.product_id,
    SUM(oi.product_price) AS total_revenue
  FROM base_orders o
  INNER JOIN base_order_items oi  -- Explicit INNER JOIN instead of comma join for clarity and optimization
    ON o.order_id = oi.order_id
  GROUP BY 1, 2, 3
)

-- Apply window function on pre-aggregated data to reduce computation
SELECT
  order_month,
  order_date,
  product_id,
  total_revenue,
  RANK() OVER (PARTITION BY order_month ORDER BY total_revenue DESC) AS rank
FROM aggregated_revenue