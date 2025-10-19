{{ config(
    materialized='incremental',
    unique_key=['order_month', 'product_id'],
    partition_by={
        'field': 'order_month_date',
        'data_type': 'date',
        'granularity': 'month'
    },
    cluster_by=['product_id'],
    incremental_strategy='merge'
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

order_items_agg AS (
    SELECT
        oi.order_id,
        oi.product_id,
        SUM(oi.product_price) AS product_revenue
    FROM {{ ref('order_items') }} oi
    {% if is_incremental() %}
    WHERE oi.order_id IN (SELECT order_id FROM orders_filtered)
    {% endif %}
    GROUP BY 1, 2
),

joined_data AS (
    SELECT
        FORMAT_TIMESTAMP('%Y-%m', o.ordered_at) AS order_month,
        PARSE_DATE('%Y-%m', FORMAT_TIMESTAMP('%Y-%m', o.ordered_at)) AS order_month_date,
        oa.product_id,
        oa.product_revenue
    FROM orders_filtered o
    INNER JOIN order_items_agg oa
        ON o.order_id = oa.order_id
),

aggregated AS (
    SELECT
        order_month,
        order_month_date,
        product_id,
        SUM(product_revenue) AS total_revenue
    FROM joined_data
    GROUP BY 1, 2, 3
)

SELECT
    order_month,
    order_month_date,
    product_id,
    total_revenue,
    RANK() OVER (PARTITION BY order_month ORDER BY total_revenue DESC) AS rank
FROM aggregated