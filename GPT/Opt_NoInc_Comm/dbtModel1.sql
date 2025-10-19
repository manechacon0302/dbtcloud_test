{{ config(
    materialized='table'
) }}

-- Add to your sources.yml:
-- sources:
--   - name: propellingtech_demo_customers
--     tables:
--       - name: fct_orders
--       - name: dim_customers

with orders as (
    select * 
    from {{ source('propellingtech_demo_customers', 'fct_orders') }}
),

customers as (
    select * 
    from {{ source('propellingtech_demo_customers', 'dim_customers') }}
),

-- Optimization: Replaced correlated subquery with a direct join for better performance on BigQuery.
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

select * from orders_with_customer;