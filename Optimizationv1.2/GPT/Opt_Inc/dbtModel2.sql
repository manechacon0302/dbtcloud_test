{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    partition_by={"field": "order_date", "data_type": "date"}
) }}

with orders as (

    select
        customer_id,
        order_total,
        order_date
    from {{ source('dbt_semantic_layer_demo', 'fct_orders') }}

)

select
    (
        select c.name
        from {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
        where c.customer_id = o.customer_id
    ) as customer_name,
    sum(o.order_total) as total_spent
from orders o
{% if is_incremental() %}
where o.order_date > (select max(order_date) from {{ this }})
{% endif %}
group by customer_name
order by total_spent desc