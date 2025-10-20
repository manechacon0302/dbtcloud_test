{{ config(
    materialized='table'
) }}

-- To add to your sources.yml file:
-- sources:
--   - name: dbt_semantic_layer_demo
--     tables:
--       - name: fct_orders
--       - name: order_items

WITH recent_orders AS (
    SELECT
        order_id,
        ordered_at
    FROM
        {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
    WHERE
        ordered_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)  -- filter pushed down to reduce scanned data
)

SELECT
    oi.product_id,
    oi.product_price,
    ro.ordered_at
FROM
    {{ source('dbt_semantic_layer_demo', 'order_items') }} oi
LEFT JOIN
    recent_orders ro
    ON ro.order_id = oi.order_id
WHERE
    ro.ordered_at IS NOT NULL  -- restrict to matching recent orders only to avoid unnecessary rows

-- Optimization notes:
-- 1. Converted RIGHT JOIN to LEFT JOIN by reversing join order for clarity and performance.
-- 2. Filtered orders before join (in CTE recent_orders) to limit data scanned/joined.
-- 3. Removed DISTINCT by applying filtering on joined table and ensuring unique keys if possible.
-- 4. Used dbt source() references for dynamic environment configs.
-- 5. Avoided DATE() function on column in WHERE clause to enable partition pruning on ordered_at (assuming ordered_at is date or timestamp).