{{ config(
    materialized='incremental',
    partition_by={"field": "order_date", "data_type": "date"}
) }}

WITH base_data AS (
    SELECT
        c.name,
        oi.order_item_id,
        oi.is_food_item,
        oi.is_drink_item,
        DATE(o.order_date) AS order_date
    FROM {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
    JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o ON c.customer_id = o.customer_id
    JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} oi ON o.order_id = oi.order_id
),
food_orders AS (
    SELECT
        name,
        COUNT(order_item_id) AS item_count,
        order_date
    FROM base_data
    WHERE is_food_item = 1
    GROUP BY name, order_date
),
drink_orders AS (
    SELECT
        name,
        COUNT(order_item_id) AS item_count,
        order_date
    FROM base_data
    WHERE is_drink_item = 1
    GROUP BY name, order_date
),
combined AS (
    SELECT
        name,
        item_count,
        'food' AS item_type,
        order_date
    FROM food_orders
    UNION ALL
    SELECT
        name,
        item_count,
        'drink' AS item_type,
        order_date
    FROM drink_orders
)

SELECT
    name,
    item_count,
    item_type
FROM combined

{% if is_incremental() %}
WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}

-- Add to your sources.yml:
--    - name: dbt_semantic_layer_demo
--      tables:
--        - name: dim_customers
--        - name: fct_orders
--        - name: order_items