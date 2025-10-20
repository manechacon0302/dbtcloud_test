{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='merge',
    partition_by={
        "field": "ordered_at",
        "data_type": "date"
    }
) }}

SELECT
  oi.product_id,
  oi.product_price,
  o.ordered_at
FROM
  {{ source('dbt_semantic_layer_demo', 'order_items') }} oi
LEFT JOIN
  {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
    ON o.order_id = oi.order_id
WHERE
  o.ordered_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)

{% if is_incremental() %}
  AND o.ordered_at > (SELECT MAX(ordered_at) FROM {{ this }})
{% endif %}