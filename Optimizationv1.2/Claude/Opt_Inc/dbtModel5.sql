{{ config(
    materialized='table'
) }}

WITH high_value_customers AS (
    SELECT 
        customer_id
    FROM {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
    GROUP BY customer_id
    HAVING SUM(order_total) > 500
)

SELECT 
    c.name,
    oi.product_id,
    oi.product_price
FROM {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
INNER JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o 
    ON c.customer_id = o.customer_id
INNER JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} oi 
    ON o.order_id = oi.order_id
INNER JOIN high_value_customers hvc
    ON c.customer_id = hvc.customer_id
WHERE oi.product_price > 20