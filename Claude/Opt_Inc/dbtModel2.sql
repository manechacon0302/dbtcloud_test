{{ config(
    materialized='table'
) }}

SELECT
  c.name AS customer_name,
  SUM(o.order_total) AS total_spent
FROM {{ source('dbt_semantic_layer_demo', 'fct_orders') }} AS o
INNER JOIN {{ source('dbt_semantic_layer_demo', 'dim_customers') }} AS c
  ON c.customer_id = o.customer_id
GROUP BY c.name
ORDER BY total_spent DESC