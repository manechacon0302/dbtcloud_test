{{ config(
    materialized='table'
) }}

-- Add the following source configuration to your sources.yml file:
-- 
-- versions:
--   - name: propellingtech_demo_semantic_layer_demo
--     tables:
--       - name: dim_customers
--       - name: fct_orders

with orders as (
    select 
        customer_id, 
        order_total
    from 
        {{ source('propellingtech_demo_semantic_layer_demo', 'fct_orders') }}
),

-- Direct join to avoid scalar subquery for better performance and readability
customer_orders as (
    select
        c.name as customer_name,
        o.order_total
    from 
        orders o
    inner join 
        {{ source('propellingtech_demo_semantic_layer_demo', 'dim_customers') }} c
        on c.customer_id = o.customer_id
)

select
    customer_name,
    sum(order_total) as total_spent
from
    customer_orders
group by
    customer_name
order by
    total_spent desc



-- Optimizations applied:
-- 1. Replaced scalar subquery per row with an INNER JOIN to improve performance and reduce overhead.
-- 2. Removed unnecessary subquery wrapping around fct_orders.
-- 3. Used dbt source function for dynamic table referencing.
-- 4. Selected only required columns from fct_orders to avoid scanning unnecessary data.
-- 5. Added comments for source config requirements.
-- 6. Materialized as table (default changeable) for performance and finops considerations in BigQuery.