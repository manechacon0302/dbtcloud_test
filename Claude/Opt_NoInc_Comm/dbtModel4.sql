-- Add these entries to your sources.yml file:
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
    labels={'team': 'analytics', 'domain': 'orders'}
) }}

-- Optimized to use UNION ALL instead of UNION to avoid expensive deduplication
-- Consolidated joins into single pass with conditional aggregation to reduce data scans
-- Eliminated redundant CTEs that were scanning the same tables multiple times

WITH order_items_enriched AS (
    -- Single scan of all three tables instead of two separate scans
    -- Pre-filtering to only relevant columns to reduce memory footprint
    SELECT
        c.name,
        oi.order_item_id,
        oi.is_food_item,
        oi.is_drink_item
    FROM
        {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
        INNER JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o 
            ON c.customer_id = o.customer_id
        INNER JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} oi 
            ON o.order_id = oi.order_id
    WHERE
        -- Filter early to reduce data volume through joins
        (oi.is_food_item = 1 OR oi.is_drink_item = 1)
),

aggregated_counts AS (
    -- Single aggregation pass using conditional logic instead of separate CTEs
    SELECT
        name,
        COUNTIF(is_food_item = 1) AS food_count,
        COUNTIF(is_drink_item = 1) AS drink_count
    FROM
        order_items_enriched
    GROUP BY
        name
)

-- Unpivot using UNION ALL (not UNION) since food and drink are mutually exclusive
-- UNION ALL avoids expensive DISTINCT operation
SELECT
    name,
    food_count AS item_count,
    'food' AS item_type
FROM
    aggregated_counts
WHERE
    food_count > 0

UNION ALL

SELECT
    name,
    drink_count AS item_count,
    'drink' AS item_type
FROM
    aggregated_counts
WHERE
    drink_count > 0