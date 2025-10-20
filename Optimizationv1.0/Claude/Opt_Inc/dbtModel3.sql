{{ config(
    materialized='incremental',
    unique_key=['product_id', 'ordered_at'],
    partition_by={
        'field': 'ordered_at',
        'data_type': 'timestamp',
        'granularity': 'day'
    },
    cluster_by=['product_id']
) }}

WITH fct_orders AS (
    SELECT
        order_id,
        ordered_at
    FROM {{ ref('fct_orders') }}
    WHERE ordered_at IS NOT NULL
    {% if is_incremental() %}
        AND ordered_at > (SELECT MAX(ordered_at) FROM {{ this }})
    {% endif %}
),

order_items AS (
    SELECT
        order_id,
        product_id,
        product_price
    FROM {{ ref('order_items') }}
)

SELECT DISTINCT
    oi.product_id,
    oi.product_price,
    o.ordered_at
FROM fct_orders o
INNER JOIN order_items oi
    ON o.order_id = oi.order_id

{% if is_incremental() %}
WHERE o.ordered_at > (SELECT MAX(ordered_at) FROM {{ this }})
{% endif %}