{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert',
    partition_by={"field": "ordered_at", "data_type": "timestamp"}
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
    {% if is_incremental() %}
        where o.ordered_at > (select coalesce(max(ordered_at), '1900-01-01') from {{ this }})
    {% endif %}
)

select * from orders_with_customer;