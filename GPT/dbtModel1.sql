{{ config(
    materialized='incremental',
    unique_key='order_id',
    post_hook=['alter table {{ this }} cluster by (created_at)']
) }}

with orders as (
    select * from {{ source('shop', 'orders') }}
    {% if is_incremental() %}
    where created_at > (select max(created_at) from {{ this }})
    {% endif %}
),
orders_with_customer as (
    select
        o.order_id,
        o.customer_id,
        c.customer_name,
        o.total_amount,
        o.created_at
    from orders o
    left join {{ source('shop', 'customers') }} c
        on o.customer_id = c.customer_id
)
select * from orders_with_customer;