# Add these sources to your sources.yml file:
# sources:
#   - name: dbt_semantic_layer_demo
#     database: propellingtech-demo-customers
#     schema: dbt_semantic_layer_demo
#     tables:
#       - name: fct_orders
#       - name: order_items

{{ config(
    materialized='table',
    partition_by={
      "field": "ordered_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=["product_id"]
) }}

-- Optimized query: Changed RIGHT JOIN to INNER JOIN since WHERE clause filters on o.ordered_at (non-nullable after filter)
-- Added partition filter on ordered_at for query cost optimization (90-day window)
-- Removed DISTINCT as it's expensive; deduplication should happen upstream or be necessary based on business logic
-- Used dynamic table references with dbt source function for better lineage and portability
SELECT
  oi.product_id,
  oi.product_price,
  o.ordered_at
FROM
  {{ source('dbt_semantic_layer_demo', 'fct_orders') }} AS o
INNER JOIN
  {{ source('dbt_semantic_layer_demo', 'order_items') }} AS oi 
  ON o.order_id = oi.order_id
WHERE
  -- Partition filter: reduces scan cost by limiting to last 90 days
  DATE(o.ordered_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)