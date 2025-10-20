/*
Add the following to your `sources.yml` file to enable dynamic source references:

sources:
  - name: propellingtech_demo_customers
    database: propellingtech-demo-customers
    schema: dbt_semantic_layer_demo
    tables:
      - name: fct_orders
      - name: dim_customers
*/

{{ config(materialized='table') }}

with orders as (
    select * from {{ source('propellingtech_demo_customers', 'fct_orders') }}
),

customers as (
    select * from {{ source('propellingtech_demo_customers', 'dim_customers') }}
),

orders_with_customer as (
    select
        o.order_id,
        o.customer_id,
        c.name as customer_name,  -- Replaced scalar subquery with join for performance optimization
        o.order_total,
        o.ordered_at
    from orders o
    left join customers c
        on o.customer_id = c.customer_id
)

select * from orders_with_customer;