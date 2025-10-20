-- Add to your sources.yml file:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     tables:
--       - name: fct_orders
--       - name: order_items

{{ config(
    materialized='incremental',
    unique_key=['order_month', 'product_id'],
    on_schema_change='fail',
    partition_by={
      "field": "ordered_at_date",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by=['product_id']
) }}

-- Optimized query with the following improvements:
-- 1. Replaced implicit JOIN with explicit INNER JOIN for better readability and performance
-- 2. Added incremental logic to process only new/updated records based on ordered_at
-- 3. Applied partitioning by date field (ordered_at_date) for BigQuery optimization
-- 4. Added clustering by product_id to improve query performance on filtered reads
-- 5. Replaced hard-coded table references with dbt ref() function for proper dependency management
-- 6. Added ordered_at_date field to enable partitioning (required for incremental strategy)
-- 7. Fixed GROUP BY to include all non-aggregated columns (product_id was missing)

SELECT
  FORMAT_TIMESTAMP('%Y-%m', o.ordered_at) AS order_month,
  oi.product_id,
  SUM(oi.product_price) AS total_revenue,
  RANK() OVER (
    PARTITION BY FORMAT_TIMESTAMP('%Y-%m', o.ordered_at) 
    ORDER BY SUM(oi.product_price) DESC
  ) AS rank,
  -- Adding date field for partitioning strategy
  DATE(o.ordered_at) AS ordered_at_date
FROM
  {{ source('dbt_semantic_layer_demo', 'fct_orders') }} AS o
  -- Using explicit INNER JOIN instead of implicit comma join for better performance
  INNER JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} AS oi
    ON o.order_id = oi.order_id
-- Incremental filter to process only new records since last run
{% if is_incremental() %}
WHERE DATE(o.ordered_at) > (SELECT MAX(ordered_at_date) FROM {{ this }})
{% endif %}
GROUP BY 
  order_month,
  product_id,
  ordered_at_date