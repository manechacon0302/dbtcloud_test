{{ config(
    materialized='incremental',
    unique_key=['name', 'item_type'],
    incremental_strategy='merge',
    partition_by={'field': 'ingestion_date', 'data_type': 'date'}
) }}

WITH base_data AS (
    SELECT
        c.name,
        oi.order_item_id,
        oi.is_food_item,
        oi.is_drink_item,
        DATE(o.order_date) AS order_date
    FROM
        {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
        JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o ON c.customer_id = o.customer_id
        JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} oi ON o.order_id = oi.order_id
),
food_orders AS (
    SELECT
        name,
        COUNT(order_item_id) AS item_count,
        DATE(order_date) AS ingestion_date,
        'food' AS item_type
    FROM
        base_data
    WHERE
        is_food_item = 1
    GROUP BY
        name,
        ingestion_date
),
drink_orders AS (
    SELECT
        name,
        COUNT(order_item_id) AS item_count,
        DATE(order_date) AS ingestion_date,
        'drink' AS item_type
    FROM
        base_data
    WHERE
        is_drink_item = 1
    GROUP BY
        name,
        ingestion_date
)

SELECT
    name,
    item_count,
    item_type,
    ingestion_date
FROM
    food_orders

UNION ALL

SELECT
    name,
    item_count,
    item_type,
    ingestion_date
FROM
    drink_orders

{% if is_incremental() %}
WHERE ingestion_date > (SELECT COALESCE(MAX(ingestion_date), '1970-01-01') FROM {{ this }})
{% endif %}