{{ config(
    materialized='table'
) }}

/*
Add these to your `sources.yml` file:

sources:
  - name: dbt_semantic_layer_demo
    database: propellingtech-demo-customers
    schema: dbt_semantic_layer_demo
    tables:
      - name: fct_orders
      - name: dim_customers
*/

with orders as (
    select * from {{ source('dbt_semantic_layer_demo', 'fct_orders') }}  -- use dbt source for better management & performance
),
customers as (
    select * from {{ source('dbt_semantic_layer_demo', 'dim_customers') }}  -- use dbt source for better management & performance
),
orders_with_customer as (
    select
        o.order_id,
        o.customer_id,
        c.name as customer_name,  -- replaced scalar subquery with JOIN for better performance
        o.order_total,
        o.ordered_at
    from orders o
    left join customers c
      on o.customer_id = c.customer_id
)

select * from orders_with_customer;