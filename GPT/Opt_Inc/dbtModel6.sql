{{ 
    config(
      materialized='incremental',
      unique_key=['order_month', 'product_id'],
      partition_by={"field": "order_month", "data_type": "date"}
    ) 
}}

WITH base AS (
    SELECT
        DATE_TRUNC(DATE(o.ordered_at), MONTH) AS order_month,
        oi.product_id,
        oi.product_price
    FROM {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
    JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} oi
      ON o.order_id = oi.order_id
)

SELECT
    order_month,
    product_id,
    SUM(product_price) AS total_revenue,
    RANK() OVER (
        PARTITION BY order_month
        ORDER BY SUM(product_price) DESC
    ) AS rank
FROM base
{% if is_incremental() %}
WHERE order_month >= (SELECT MAX(order_month) FROM {{ this }})
{% endif %}
GROUP BY 1, 2
ORDER BY 1, rank

/* 
Add to your sources.yml:

sources:
  - name: dbt_semantic_layer_demo
    tables:
      - name: fct_orders
      - name: order_items
*/