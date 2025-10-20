{{ config(
    materialized='table',
    partition_by={
        "field": "last_order_date",
        "data_type": "date"
    },
    cluster_by=["customer_name"]
) }}

WITH orders_agg AS (
  SELECT
    customer_id,
    SUM(order_total) AS total_spent,
    MAX(ordered_at) AS last_order_date
  FROM
    {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
  GROUP BY
    customer_id
),

customers AS (
  SELECT
    customer_id,
    name AS customer_name
  FROM
    {{ source('dbt_semantic_layer_demo', 'dim_customers') }}
)

SELECT
  c.customer_name,
  o.total_spent,
  o.last_order_date
FROM
  orders_agg o
INNER JOIN
  customers c
ON
  o.customer_id = c.customer_id
ORDER BY
  o.total_spent DESC