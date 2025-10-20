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
      "field": "ordered_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=['product_id'],
    on_schema_change='sync_all_columns'
  )
}}

-- Optimized query: Changed RIGHT JOIN to LEFT JOIN for better performance and correct logic
-- Added incremental strategy to process only new/updated records
-- Applied clustering on product_id for improved query performance on filtered queries
-- Removed DISTINCT and using GROUP BY for better performance in BigQuery
-- Added dynamic table references using dbt source function
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
  -- Initial load: last 90 days of data
  {% if not is_incremental() %}
  AND DATE(o.ordered_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  {% else %}
  -- Incremental load: only process records newer than the max date in target table
  AND DATE(o.ordered_at) >= (SELECT DATE_SUB(MAX(DATE(ordered_at)), INTERVAL 1 DAY) FROM {{ this }})
  {% endif %}
GROUP BY
  oi.product_id,
  oi.product_price,
  o.ordered_at