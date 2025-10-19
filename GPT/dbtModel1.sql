{{
  config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
  )
}}

with orders as (
    select
        order_id,
        customer_id,
        order_total,
        ordered_at
    from {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
    {% if is_incremental() %}
      where ordered_at > (select max(ordered_at) from {{ this }})
    {% endif %}
),
customers as (
    select
        customer_id,
        name as customer_name
    from {{ source('dbt_semantic_layer_demo', 'dim_customers') }}
),
orders_with_customer as (
    select
        o.order_id,
        o.customer_id,
        c.customer_name,
        o.order_total,
        o.ordered_at
    from orders o
    left join customers c
        on o.customer_id = c.customer_id
)
select * from orders_with_customer;