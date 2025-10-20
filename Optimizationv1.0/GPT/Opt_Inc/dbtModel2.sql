{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge'
) }}

with orders as (
    select
        customer_id,
        order_total,
        order_date
    from {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
    {% if is_incremental() %}
      where order_date > (select max(order_date) from {{ this }})
    {% endif %}
),

customer_orders as (
    select
        o.customer_id,
        sum(o.order_total) as total_spent
    from orders o
    group by o.customer_id
)

select
    c.name as customer_name,
    co.total_spent
from customer_orders co
join {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
  on c.customer_id = co.customer_id
order by
    total_spent desc

/*
Add these source definitions to your sources.yml if not already present:

version: 2

sources:
  - name: dbt_semantic_layer_demo
    database: propellingtech-demo-customers
    schema: dbt_semantic_layer_demo
    tables:
      - name: fct_orders
      - name: dim_customers
*/