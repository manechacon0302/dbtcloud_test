-- Add to your sources.yml:
-- 
-- sources:
--   - name: semantic_layer_demo
--     database: propellingtech-demo-customers
--     schema: dbt_semantic_layer_demo
--     tables:
--       - name: dim_customers
--       - name: fct_orders

{{ config(materialized='table') }}

with orders as (
    select
        customer_id,
        order_total
    from {{ source('semantic_layer_demo', 'fct_orders') }}
),

customers as (
    select
        customer_id,
        name as customer_name
    from {{ source('semantic_layer_demo', 'dim_customers') }}
)

select
    c.customer_name,
    sum(o.order_total) as total_spent
from orders o
join customers c
    on o.customer_id = c.customer_id
group by 1
order by total_spent desc

-- Optimizations applied:
-- 1. Replaced correlated subquery with a join to avoid row-by-row lookup and improve performance.
-- 2. Removed unnecessary subquery wrapping fct_orders.
-- 3. Used dbt source() macros for dynamic table referencing.
-- 4. Selected only necessary columns instead of SELECT * to reduce data scanned.
-- 5. Used numeric group by reference (group by 1) for clarity and brevity.
-- 6. Configured model as table materialization for optimized query performance on BigQuery.