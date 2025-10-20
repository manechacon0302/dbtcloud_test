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
    materialized='incremental',
    unique_key='product_id',
    partition_by={
      'field': 'ordered_at',
      'data_type': 'timestamp',
      'granularity': 'day'
    },
    cluster_by=['product_id'],
    on_schema_change='sync_all_columns'
  )
}}

-- Optimized query: Changed RIGHT JOIN to LEFT JOIN for better performance
-- Added incremental logic to process only new/updated records
-- Removed DISTINCT as it's expensive; using unique_key in config instead
-- Removed WHERE clause filter in incremental runs to rely on dbt's is_incremental() macro
SELECT
  oi.product_id,
  oi.product_price,
  o.ordered_at
FROM
  {{ source('dbt_semantic_layer_demo', 'order_items') }} oi
LEFT JOIN
  {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
    ON oi.order_id = o.order_id
WHERE
  o.ordered_at IS NOT NULL
  -- Initial load: get last 90 days of data
  {% if not is_incremental() %}
    AND DATE(o.ordered_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  -- Incremental runs: only process new records since last run
  {% else %}
    AND DATE(o.ordered_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
  {% endif %}