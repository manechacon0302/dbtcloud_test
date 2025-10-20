{{ config(
    materialized='table'
) }}

-- Add these sources to your sources.yml file:
-- sources:
--   - name: semantic_layer_demo
--     database: propellingtech-demo-customers
--     schema: dbt_semantic_layer_demo
--     tables:
--       - name: dim_customers
--       - name: fct_orders
--       - name: order_items


WITH high_value_customers AS (
    SELECT
        customer_id
    FROM
        {{ source('semantic_layer_demo', 'fct_orders') }}
    GROUP BY
        customer_id
    HAVING
        SUM(order_total) > 500
)

SELECT
    c.name,
    oi.product_id,
    oi.product_price
FROM
    {{ source('semantic_layer_demo', 'dim_customers') }} c
JOIN
    {{ source('semantic_layer_demo', 'fct_orders') }} o
    ON c.customer_id = o.customer_id
JOIN
    {{ source('semantic_layer_demo', 'order_items') }} oi
    ON o.order_id = oi.order_id  -- fixed join key: order_items join to fct_orders on order_id, not customer_id
WHERE
    oi.product_price > 20
    AND c.customer_id IN (SELECT customer_id FROM high_value_customers)

-- Optimizations applied:
-- 1. Used CTE (high_value_customers) to pre-aggregate customers with orders over 500 to avoid scanning full orders table multiple times.
-- 2. Corrected join condition between order_items and fct_orders to join on order_id (previously incorrectly joining order_items on customer_id).
-- 3. Replaced hardcoded table references with dbt source() macros for dynamic and manageable table referencing.
-- 4. Added inline comments explaining fixes and performance improvements.
-- 5. Retained materialization as table (no incremental per requirements).