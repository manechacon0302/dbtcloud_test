```
-- Add these source definitions to your schema.yml file:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     schema: dbt_semantic_layer_demo
--     tables:
--       - name: dim_customers
--       - name: fct_orders

{{ config(
    materialized='table',
    partition_by={
      "field": "order_date",
      "data_type": "date"
    } if 'order_date' in adapter.get_columns_in_relation(ref('fct_orders')) else none,
    cluster_by=['customer_id']
) }}

-- Optimized query: Replaced correlated subquery with JOIN for better performance
-- Removed unnecessary nested SELECT from fct_orders
-- Using dynamic table references with dbt source function for better dependency management
WITH base_orders AS (
  SELECT
    customer_id,
    order_total
  FROM
    {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
),

customers AS (
  SELECT
    customer_id,
    name
  FROM
    {{ source('dbt_semantic_layer_demo', 'dim_customers') }}
)

-- JOIN instead of correlated subquery reduces BigQuery slot usage and scan costs
SELECT
  c.name AS customer_name,
  SUM(o.order_total) AS total_spent
FROM
  base_orders o
INNER JOIN
  customers c
  ON o.customer_id = c.customer_id
GROUP BY
  c.name
ORDER BY
  total_spent DESC
```