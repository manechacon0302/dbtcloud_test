{{ config(
    materialized='incremental',
    unique_key=['name', 'item_type']
) }}

WITH base_data AS (
    SELECT
        c.name,
        oi.order_item_id,
        oi.is_food_item,
        oi.is_drink_item
    FROM {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
    JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o ON c.customer_id = o.customer_id
    JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} oi ON o.order_id = oi.order_id
    {% if is_incremental() %}
        WHERE o.order_date > (SELECT MAX(o2.order_date) FROM {{ this }} t2 JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o2 ON t2.name = c.name)
    {% endif %}
),
food_orders AS (
    SELECT
        name,
        COUNT(order_item_id) AS item_count,
        'food' AS item_type
    FROM base_data
    WHERE is_food_item = 1
    GROUP BY name
),
drink_orders AS (
    SELECT
        name,
        COUNT(order_item_id) AS item_count,
        'drink' AS item_type
    FROM base_data
    WHERE is_drink_item = 1
    GROUP BY name
)

SELECT * FROM food_orders
UNION ALL
SELECT * FROM drink_orders

/* 
Add to your sources.yml:

- name: dbt_semantic_layer_demo
  tables:
    - name: dim_customers
    - name: fct_orders
    - name: order_items
*/