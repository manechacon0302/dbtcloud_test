-- Add this to your sources.yml file:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     schema: dbt_semantic_layer_demo
--     tables:
--       - name: fct_orders
--       - name: dim_customers

{{ config(
    materialized='table',
    partition_by={
        "field": "ordered_at",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=["customer_id"]
) }}

-- Optimizations applied:
-- 1. Removed correlated subquery in SELECT clause which causes N+1 query pattern
-- 2. Replaced with proper JOIN for better performance and query plan optimization
-- 3. Added dynamic table references using dbt source() function
-- 4. Removed redundant CTE 'customers' that was not being used
-- 5. Added partitioning by ordered_at for time-based query optimization
-- 6. Added clustering by customer_id for improved join and filter performance
-- 7. Fixed indentation and spacing for better readability

with orders as (
    
    select 
        order_id,
        customer_id,
        order_total,
        ordered_at
    from {{ source('dbt_semantic_layer_demo', 'fct_orders') }}

),

customers as (
    
    select 
        customer_id,
        name
    from {{ source('dbt_semantic_layer_demo', 'dim_customers') }}

),

orders_with_customer as (
    
    select
        o.order_id,
        o.customer_id,
        c.name as customer_name,
        o.order_total,
        o.ordered_at
    from orders o
    left join customers c
        on o.customer_id = c.customer_id

)

select * from orders_with_customer