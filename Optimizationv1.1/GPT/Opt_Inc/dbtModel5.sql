{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'order_date', 'data_type': 'date'}
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
JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
    ON c.customer_id = o.customer_id
JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} oi
    ON o.order_id = oi.order_id
WHERE
    oi.product_price > 20
    AND c.customer_id IN (SELECT customer_id FROM high_value_customers)

{% if is_incremental() %}
AND o.order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}