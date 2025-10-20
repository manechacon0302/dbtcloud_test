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

WITH orders_filtered AS (
    SELECT
        order_id,
        ordered_at
    FROM {{ ref('fct_orders') }}
    {% if is_incremental() %}
    WHERE ordered_at >= (SELECT DATE_SUB(MAX(order_month_date), INTERVAL 2 MONTH) FROM {{ this }})
    {% endif %}
),

order_items_filtered AS (
    SELECT
        order_id,
        product_id,
        product_price
    FROM {{ ref('order_items') }}
),

aggregated_data AS (
    SELECT
        FORMAT_TIMESTAMP('%Y-%m', o.ordered_at) AS order_month,
        DATE_TRUNC(DATE(o.ordered_at), MONTH) AS order_month_date,
        oi.product_id,
        SUM(oi.product_price) AS total_revenue
    FROM orders_filtered o
    INNER JOIN order_items_filtered oi
        ON o.order_id = oi.order_id
    GROUP BY 1, 2, 3
)

SELECT
    order_month,
    order_month_date,
    product_id,
    total_revenue,
    RANK() OVER (PARTITION BY order_month ORDER BY total_revenue DESC) AS rank
FROM aggregated_data