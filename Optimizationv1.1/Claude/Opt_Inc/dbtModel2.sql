{{ config(
    materialized='table',
    partition_by={
      "field": "last_order_date",
      "data_type": "date"
    }
) }}

WITH orders AS (
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
),

customer_orders AS (
  SELECT
    c.name AS customer_name,
    SUM(o.order_total) AS total_spent,
    MAX(o.order_date) AS last_order_date
  FROM
    orders o
  INNER JOIN
    customers c
  ON
    o.customer_id = c.customer_id
  GROUP BY
    c.name
)

SELECT
  customer_name,
  total_spent,
  last_order_date
FROM
  customer_orders
ORDER BY
  total_spent DESC