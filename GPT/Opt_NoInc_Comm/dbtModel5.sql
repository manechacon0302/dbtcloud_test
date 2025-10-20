-- Add these sources to your sources.yml:
-- 
-- version: 2
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     schema: dbt_semantic_layer_demo
--     tables:
--       - name: dim_customers
--       - name: fct_orders
--       - name: order_items

{{ config(materialized='table') }}

with high_value_customers as (
    -- Aggregate order_total by customer_id to filter customers with total orders over 500
    select
        customer_id
    from {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
    group by customer_id
    having sum(order_total) > 500
),

orders_with_items as (
    select
        o.customer_id,
        oi.product_id,
        oi.product_price,
        oi.order_id
    from {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
    join {{ source('dbt_semantic_layer_demo', 'order_items') }} oi
        on o.order_id = oi.order_id  -- corrected ON condition from original (was wrong join key)
    where oi.product_price > 20  -- filter early to reduce data processed
),

filtered_order_items as (
    select
        owi.customer_id,
        owi.product_id,
        owi.product_price
    from orders_with_items owi
    join high_value_customers hvc
        on owi.customer_id = hvc.customer_id -- only keep high value customers
)

select
    c.name,
    foi.product_id,
    foi.product_price
from filtered_order_items foi
join {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
    on foi.customer_id = c.customer_id

-- Optimization notes:
-- 1. Fixed incorrect JOIN condition: order_items join was on c.customer_id = oi.order_id (incorrect), changed to proper o.order_id = oi.order_id
-- 2. Used CTEs to break down logic for improved readability and performance.
-- 3. Early filtering on product_price > 20 at the lowest possible step reduces processed data.
-- 4. Filtered customers with sum(order_total) > 500 first, then joined only those customers.
-- 5. Used 3-step joins to avoid repeated scanning of large tables.
-- 6. All table references use dbt source function for dynamic references.
-- 7. Materialization set to 'table' as requested (no incremental).