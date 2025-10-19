{{ config(
    materialized='incremental',
    unique_key='order_id'
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
    on o.customer_id = c.customer_id
)
select *
from orders_with_customer
{% if is_incremental() %}
where ordered_at > (select max(ordered_at) from {{ this }})
{% endif %}

-- Add to your sources.yml file:
-- 
-- version: 2
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     schema: dbt_semantic_layer_demo
--     tables:
--       - name: fct_orders
--       - name: dim_customers