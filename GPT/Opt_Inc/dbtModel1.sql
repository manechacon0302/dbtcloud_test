{{ config(
    materialized='incremental',
    unique_key='order_id',
    partition_by={
        "field": "ordered_at",
        "data_type": "date"
    }
) }}

with orders as (
    select * from {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
),
orders_with_customer as (
    select
        o.order_id,
        o.customer_id,
        c.name as customer_name,
        o.order_total,
        o.ordered_at
    from orders o
    left join {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
        on c.customer_id = o.customer_id
)

select * from orders_with_customer

{% if is_incremental() %}
  where ordered_at > (select max(ordered_at) from {{ this }})
{% endif %}