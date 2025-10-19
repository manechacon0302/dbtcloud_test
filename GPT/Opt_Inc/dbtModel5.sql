{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='merge'
) }}

WITH high_value_customers AS (
    SELECT 
        customer_id
    FROM 
        {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
    GROUP BY 
        customer_id
    HAVING 
        SUM(order_total) > 500
)

SELECT 
    c.name,
    oi.product_id,
    oi.product_price
FROM 
    {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
JOIN 
    {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
    ON c.customer_id = o.customer_id
JOIN 
    {{ source('dbt_semantic_layer_demo', 'order_items') }} oi
    ON o.order_id = oi.order_id
JOIN
    high_value_customers hvc
    ON c.customer_id = hvc.customer_id
WHERE 
    oi.product_price > 20
{% if is_incremental() %}
    AND o.order_date > (SELECT COALESCE(MAX(o2.order_date), '1900-01-01') FROM {{ this }} o2)
{% endif %}