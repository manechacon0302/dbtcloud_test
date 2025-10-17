```sql
{{ 
  config(
    materialized='incremental',
    unique_key='order_id',
    partition_by={"field": "created_at", "data_type": "timestamp"},
    cluster_by=['customer_id']
  ) 
}}

with orders as (
    select * from {{ source('shop', 'orders') }}
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
      on c.customer_id = o.customer_id
)

select * from orders_with_customer

{% if is_incremental() %}
  where created_at > (select max(created_at) from {{ this }})
{% endif %}
```