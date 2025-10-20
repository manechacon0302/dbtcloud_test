{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='product_id'
) }}

WITH filtered_orders AS (
    SELECT
        order_id,
        ordered_at
    FROM {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
    WHERE ordered_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
)

SELECT
    oi.product_id,
    oi.product_price,
    fo.ordered_at
FROM {{ source('dbt_semantic_layer_demo', 'order_items') }} oi
RIGHT JOIN filtered_orders fo ON fo.order_id = oi.order_id

{% if is_incremental() %}

WHERE fo.ordered_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)

{% endif %}