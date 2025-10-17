with orders as (
    select * from {{ source('shop', 'orders') }}
),
customers as (
    select * from {{ source('shop', 'customers') }}
),
orders_with_customer as (
    select
        o.order_id,
        o.customer_id,
        (select c.customer_name from {{ source('shop', 'customers') }} c where c.customer_id = o.customer_id) as customer_name,
        o.total_amount,
        o.created_at
    from orders o
)
select * from orders_with_customer;
