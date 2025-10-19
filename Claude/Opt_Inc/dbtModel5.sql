{{ config(
    materialized='table',
    partition_by={
        'field': 'created_at',
        'data_type': 'timestamp',
        'granularity': 'day'
    },
    cluster_by=['customer_id', 'product_id']
) }}

WITH high_value_customers AS (
    SELECT 
        customer_id
    FROM {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
    GROUP BY customer_id
    HAVING SUM(order_total) > 500
),

order_items_filtered AS (
    SELECT 
        order_id,
        product_id,
        product_price
    FROM {{ source('dbt_semantic_layer_demo', 'order_items') }}
    WHERE product_price > 20
)

SELECT 
    c.name,
    c.customer_id,
    oi.product_id,
    oi.product_price,
    CURRENT_TIMESTAMP() AS created_at
FROM {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
INNER JOIN high_value_customers hvc
    ON c.customer_id = hvc.customer_id
INNER JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
    ON c.customer_id = o.customer_id
INNER JOIN order_items_filtered oi
    ON o.order_id = oi.order_id