{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge',
    partition_by={"field": "order_date", "data_type": "date"}
) }}

WITH orders AS (
    SELECT
      customer_id,
      order_total,
      order_date
    FROM {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
    {% if is_incremental() %}
      WHERE order_date >= (SELECT MAX(order_date) FROM {{ this }})
    {% endif %}
),

customer_names AS (
    SELECT
      customer_id,
      name AS customer_name
    FROM {{ source('dbt_semantic_layer_demo', 'dim_customers') }}
)

SELECT
  o.customer_id,
  c.customer_name,
  SUM(o.order_total) AS total_spent
FROM orders o
LEFT JOIN customer_names c
  ON o.customer_id = c.customer_id
GROUP BY
  o.customer_id,
  c.customer_name
ORDER BY
  total_spent DESC;