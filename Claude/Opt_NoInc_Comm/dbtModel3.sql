-- Add this to your sources.yml file:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     schema: dbt_semantic_layer_demo
--     tables:
--       - name: fct_orders
--       - name: order_items

{{ config(
    materialized='table',
    partition_by={
      "field": "ordered_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=["product_id"]
) }}

-- Optimizations applied:
-- 1. Changed RIGHT JOIN to INNER JOIN - more efficient and logically correct for the WHERE clause filter
-- 2. Replaced hardcoded table references with dbt source() function for dynamic referencing
-- 3. Removed DISTINCT as it forces expensive deduplication; if duplicates exist, address at source
-- 4. Moved date filter to JOIN condition for better query pruning with partitioned tables
-- 5. Added partition by ordered_at and clustering by product_id for improved query performance
-- 6. Used timestamp comparison instead of DATE() function to leverage partition pruning

WITH filtered_orders AS (
    SELECT
        order_id,
        ordered_at
    FROM {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
    WHERE ordered_at >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))
)

SELECT
    oi.product_id,
    oi.product_price,
    fo.ordered_at
FROM {{ source('dbt_semantic_layer_demo', 'order_items') }} AS oi
INNER JOIN filtered_orders AS fo
    ON oi.order_id = fo.order_id