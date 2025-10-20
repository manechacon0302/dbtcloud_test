{{ config(
    materialized='incremental',
    unique_key=['order_month', 'product_id'],
    on_schema_change='sync_all_columns',
    partition_by={
        'field': 'order_month_date',
        'data_type': 'date',
        'granularity': 'month'
    }
) }}

WITH order_data AS (
    SELECT
        o.order_id,
        o.ordered_at,
        oi.product_id,
        oi.product_price
    FROM {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
    INNER JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} oi
        ON o.order_id = oi.order_id
    WHERE TRUE
        {% if is_incremental() %}
        AND o.ordered_at >= (SELECT COALESCE(MAX(order_month_date), '1900-01-01') FROM {{ this }})
        {% endif %}
),

aggregated_data AS (
    SELECT
        FORMAT_TIMESTAMP('%Y-%m', ordered_at) AS order_month,
        product_id,
        SUM(product_price) AS total_revenue
    FROM order_data
    GROUP BY 1, 2
)

SELECT
    order_month,
    DATE(PARSE_TIMESTAMP('%Y-%m', order_month)) AS order_month_date,
    product_id,
    total_revenue,
    RANK() OVER (PARTITION BY order_month ORDER BY total_revenue DESC) AS rank
FROM aggregated_data