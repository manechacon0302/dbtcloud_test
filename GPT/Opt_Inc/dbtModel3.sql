{{ config(
    materialized = "incremental",
    incremental_strategy = "merge",
    unique_key = "product_id",
    partition_by = "DATE(ordered_at)"
) }}

SELECT
  oi.product_id,
  oi.product_price,
  o.ordered_at
FROM
  {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
RIGHT JOIN
  {{ source('dbt_semantic_layer_demo', 'order_items') }} oi ON o.order_id = oi.order_id
WHERE
  DATE(o.ordered_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)

{% if is_incremental() %}
  AND DATE(o.ordered_at) > (SELECT MAX(DATE(ordered_at)) FROM {{ this }})
{% endif %}