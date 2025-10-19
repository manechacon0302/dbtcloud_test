-- Add the following to your sources.yml file:
-- 
-- sources:
--   - name: semantic_layer_demo
--     tables:
--       - name: fct_orders
--       - name: order_items

{{ config(materialized='table') }}

with filtered_orders as (
    select
        order_id,
        ordered_at
    from {{ source('semantic_layer_demo', 'fct_orders') }}
    where ordered_at >= date_sub(current_date(), interval 90 day)
)

select distinct
    oi.product_id,
    oi.product_price,
    fo.ordered_at
from {{ source('semantic_layer_demo', 'order_items') }} oi
left join filtered_orders fo
    on fo.order_id = oi.order_id

-- Optimization notes:
-- 1. Replaced hardcoded table references with dbt source() for maintainability.
-- 2. Filtered orders first to limit the dataset before the join, optimizing performance.
-- 3. Changed RIGHT JOIN to LEFT JOIN by starting from order_items, as it is more natural and efficient in BigQuery.
-- 4. Removed unnecessary DATE() conversion in WHERE clause by filtering directly on ordered_at datetime.
-- 5. Selected only required columns in the CTE to reduce data scanned.
-- 6. Added comments for source configuration to guide users on updating sources.yml file.