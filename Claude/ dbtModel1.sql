{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='sync_all_columns',
    partition_by={
        'field': 'created_at',
        'data_type': 'timestamp',
        'granularity': 'day'
    },
    cluster_by=['customer_id', 'created_at']
) }}

with orders as (
    select * from {{ source('shop', 'orders') }}
    {% if is_incremental() %}
    where created_at > (select max(created_at) from {{ this }})
    {% endif %}
),
customers as (
    select 
        customer_id,
        customer_name
    from {{ source('shop', 'customers') }}
),
orders_with_customer as (
    select
        o.order_id,
        o.customer_id,
        c.customer_name,
        o.total_amount,
        o.created_at
    from orders o
    left join customers c
        on o.customer_id = c.customer_id
)
select * from orders_with_customer