{{ config(
    materialized='table',
    partition_by={
      "field": "order_date",
      "data_type": "date"
    }
) }}

WITH high_value_customers AS (
    SELECT 
        customer_id
    FROM {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
    GROUP BY customer_id
    HAVING SUM(order_total) > 500
),

filtered_order_items AS (
    SELECT 
        order_id,
        product_id,
        product_price
    FROM {{ source('dbt_semantic_layer_demo', 'order_items') }}
    WHERE product_price > 20
)

SELECT 
    c.name,
    oi.product_id,
    oi.product_price,
    o.order_date
FROM {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
INNER JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
    ON c.customer_id = o.customer_id
INNER JOIN filtered_order_items oi
    ON o.order_id = oi.order_id
INNER JOIN high_value_customers hvc
    ON c.customer_id = hvc.customer_id