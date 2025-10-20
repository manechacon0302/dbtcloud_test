-- Add to your sources.yml file:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     schema: dbt_semantic_layer_demo
--     tables:
--       - name: fct_orders
--       - name: dim_customers

{{
    config(
        materialized='incremental',
        unique_key='order_id',
        partition_by={
            'field': 'ordered_at',
            'data_type': 'timestamp',
            'granularity': 'day'
        },
        cluster_by=['customer_id']
    )
}}

with orders as (
    select 
        order_id,
        customer_id,
        order_total,
        ordered_at
    from {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
    -- Incremental filter to process only new/updated records
    {% if is_incremental() %}
    where ordered_at > (select max(ordered_at) from {{ this }})
    {% endif %}
),

customers as (
    select 
        customer_id,
        name
    from {{ source('dbt_semantic_layer_demo', 'dim_customers') }}
),

-- Removed correlated subquery and replaced with efficient JOIN for better performance
orders_with_customer as (
    select
        o.order_id,
        o.customer_id,
        c.name as customer_name,
        o.order_total,
        o.ordered_at
    from orders o
    -- LEFT JOIN ensures all orders are retained even if customer data is missing
    left join customers c on c.customer_id = o.customer_id
)

select * from orders_with_customer