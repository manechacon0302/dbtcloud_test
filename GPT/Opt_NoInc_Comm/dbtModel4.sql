-- Add these source definitions to your `sources.yml` for dynamic referencing:
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

with base_orders as (
    select
        c.name,
        oi.is_food_item,
        oi.is_drink_item,
        oi.order_item_id
    from
        {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
        join {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
            on c.customer_id = o.customer_id
        join {{ source('dbt_semantic_layer_demo', 'order_items') }} oi
            on o.order_id = oi.order_id
    where
        oi.is_food_item = 1
        or oi.is_drink_item = 1
),

aggregated_orders as (
    select
        name,
        sum(case when is_food_item = 1 then 1 else 0 end) as food_item_count,
        sum(case when is_drink_item = 1 then 1 else 0 end) as drink_item_count
    from base_orders
    group by name
)

select
    name,
    food_item_count as item_count,
    'food' as item_type
from aggregated_orders
where food_item_count > 0

union all

select
    name,
    drink_item_count as item_count,
    'drink' as item_type
from aggregated_orders
where drink_item_count > 0

-- Optimization notes:
-- 1. Reduced redundant joins by combining food and drink item filtering in a single base CTE.
-- 2. Aggregated both counts in one pass to avoid double scanning same joins.
-- 3. Used UNION ALL instead of UNION as there is no overlapping data, improving performance.
-- 4. Added filtering to exclude zero-count rows to reduce output size.
-- 5. Replaced hardcoded table references with dbt source functions for better maintainability and environment flexibility.