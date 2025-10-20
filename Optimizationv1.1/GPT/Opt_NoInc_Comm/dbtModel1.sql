-- Add the following rows to your sources.yml file for dynamic references:
-- 
-- version: 2
-- sources:
--   - name: semantic_layer_demo
--     database: propellingtech-demo-customers
--     schema: dbt_semantic_layer_demo
--     tables:
--       - name: fct_orders
--       - name: dim_customers


{{ config(
    materialized='table'
) }}

with orders as (
    -- Reference fct_orders dynamically, select only needed columns for performance
    select
        order_id,
        customer_id,
        order_total,
        ordered_at
    from {{ source('semantic_layer_demo', 'fct_orders') }}
),

orders_with_customer as (
    select
        o.order_id,
        o.customer_id,
        c.name as customer_name,  -- Join instead of scalar subquery for better performance
        o.order_total,
        o.ordered_at
    from orders o
    left join {{ source('semantic_layer_demo', 'dim_customers') }} c
        on o.customer_id = c.customer_id
)

select * from orders_with_customer;

