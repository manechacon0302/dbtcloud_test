-- Add to sources.yml:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     tables:
--       - name: dim_customers
--       - name: fct_orders

{{ config(
    materialized='table',
    partition_by={
      "field": "order_date",
      "data_type": "date"
    } if 'order_date' in adapter.get_columns_in_relation(source('dbt_semantic_layer_demo', 'fct_orders')) else none,
    cluster_by=['customer_name']
) }}

-- Optimized query: replaced correlated subquery with JOIN for better performance
-- Removed unnecessary subquery wrapper around fct_orders
-- Using dbt source references for dynamic table references
SELECT
  c.name AS customer_name,
  SUM(o.order_total) AS total_spent
FROM
  {{ source('dbt_semantic_layer_demo', 'fct_orders') }} AS o
  -- LEFT JOIN ensures all orders are included even if customer data is missing
  LEFT JOIN {{ source('dbt_semantic_layer_demo', 'dim_customers') }} AS c
    ON o.customer_id = c.customer_id
GROUP BY
  c.name
ORDER BY
  total_spent DESC