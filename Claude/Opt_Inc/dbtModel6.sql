{{ config(
    materialized='incremental',
    unique_key=['order_month', 'product_id'],
    partition_by={
        'field': 'order_month_date',
        'data_type': 'date',
        'granularity': 'month'
    },
    cluster_by=['product_id']
) }}

SELECT
    FORMAT_TIMESTAMP('%Y-%m', o.ordered_at) AS order_month,
    DATE_TRUNC(DATE(o.ordered_at), MONTH) AS order_month_date,
    oi.product_id,
    SUM(oi.product_price) AS total_revenue,
    RANK() OVER (
        PARTITION BY FORMAT_TIMESTAMP('%Y-%m', o.ordered_at) 
        ORDER BY SUM(oi.product_price) DESC
    ) AS rank
FROM {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
INNER JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} oi
    ON o.order_id = oi.order_id
{% if is_incremental() %}
WHERE DATE(o.ordered_at) > (SELECT MAX(order_month_date) FROM {{ this }})
{% endif %}
GROUP BY 
    order_month,
    order_month_date,
    oi.product_id