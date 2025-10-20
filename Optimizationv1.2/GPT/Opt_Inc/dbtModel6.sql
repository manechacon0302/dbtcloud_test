{{ config(
    materialized='incremental',
    unique_key=['order_month', 'product_id'],
    partition_by={"field": "order_month", "data_type": "DATE"}
) }}

WITH base AS (
    SELECT
        DATE_TRUNC(DATE(o.ordered_at), MONTH) AS order_month,
        oi.product_id,
        oi.product_price
    FROM
        {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
    JOIN
        {{ source('dbt_semantic_layer_demo', 'order_items') }} oi
      ON o.order_id = oi.order_id
)

SELECT
    FORMAT_DATE('%Y-%m', order_month) AS order_month,
    product_id,
    SUM(product_price) AS total_revenue,
    RANK() OVER (
      PARTITION BY order_month 
      ORDER BY SUM(product_price) DESC
    ) AS rank
FROM base
GROUP BY 1, 2

{% if is_incremental() %}
WHERE order_month >= DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL -6 MONTH), MONTH)
{% endif %}