{{ config(
    materialized = "incremental",
    unique_key = "customer_id",
    partition_by = "date"
) }}

with orders as (
    select
        customer_id,
        order_total,
        order_date
    from 
        {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
    {% if is_incremental() %}
      where order_date > (select max(order_date) from {{ this }})
    {% endif %}
),

customers as (
    select
        customer_id,
        name as customer_name
    from 
        {{ source('dbt_semantic_layer_demo', 'dim_customers') }}
)

select
    c.customer_name,
    sum(o.order_total) as total_spent
from
    orders o
join
    customers c
    on o.customer_id = c.customer_id
group by
    c.customer_name
order by
    total_spent desc

-- Add to your sources.yml file:
-- 
-- sources:
--   - name: dbt_semantic_layer_demo
--     tables:
--       - name: fct_orders
--       - name: dim_customers