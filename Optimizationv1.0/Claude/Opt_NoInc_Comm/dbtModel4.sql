-- Add these sources to your dbt sources.yml file:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     schema: dbt_semantic_layer_demo
--     tables:
--       - name: dim_customers
--       - name: fct_orders
--       - name: order_items

{{ config(
    materialized='table',
    partition_by={
        "field": "item_type",
        "data_type": "string"
    },
    cluster_by=['name']
) }}

-- Optimization applied: Single scan of tables instead of two separate scans
-- Uses COUNTIF with conditional aggregation instead of multiple CTEs with WHERE clauses
-- This reduces I/O operations by ~50% and improves query performance
-- Replaced UNION with UNION ALL in the unpivot operation (no need for deduplication)
-- Added BigQuery-specific optimizations: partitioning and clustering for better query performance

WITH customer_order_items AS (
    SELECT
        c.name,
        oi.is_food_item,
        oi.is_drink_item,
        oi.order_item_id
    FROM
        {{ source('dbt_semantic_layer_demo', 'dim_customers') }} AS c
        INNER JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} AS o 
            ON c.customer_id = o.customer_id
        INNER JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} AS oi 
            ON o.order_id = oi.order_id
    WHERE
        (oi.is_food_item = 1 OR oi.is_drink_item = 1)
),

aggregated_items AS (
    SELECT
        name,
        COUNTIF(is_food_item = 1) AS food_count,
        COUNTIF(is_drink_item = 1) AS drink_count
    FROM
        customer_order_items
    GROUP BY
        name
)

-- Unpivot the aggregated results to match original output format
SELECT
    name,
    food_count AS item_count,
    'food' AS item_type
FROM
    aggregated_items
WHERE
    food_count > 0

UNION ALL

SELECT
    name,
    drink_count AS item_count,
    'drink' AS item_type
FROM
    aggregated_items
WHERE
    drink_count > 0