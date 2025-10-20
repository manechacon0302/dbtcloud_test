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

SELECT DISTINCT
  oi.product_id,
  oi.product_price,
  o.ordered_at
FROM {{ source('dbt_semantic_layer_demo', 'order_items') }} oi
LEFT JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
  ON oi.order_id = o.order_id
WHERE o.ordered_at >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))
{% if is_incremental() %}
  AND o.ordered_at > (SELECT MAX(ordered_at) FROM {{ this }})
{% endif %}