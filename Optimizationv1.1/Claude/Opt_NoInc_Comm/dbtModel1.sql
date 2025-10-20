-- Add to sources.yml:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     tables:
--       - name: fct_orders
--       - name: dim_customers

{{ config(
    materialized='table',
    partition_by={
      "field": "ordered_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=["customer_id"]
) }}

with orders as (
    -- Using source reference for better dependency management
    select 
        order_id,
        customer_id,
        order_total,
        ordered_at
    from {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
),

customers as (
    -- Using source reference and selecting only needed columns to reduce memory footprint
    select 
        customer_id,
        name
    from {{ source('dbt_semantic_layer_demo', 'dim_customers') }}
),

orders_with_customer as (
    -- Replaced correlated subquery with efficient JOIN for better performance
    -- JOIN is more efficient than row-by-row subquery execution in BigQuery
    select
        o.order_id,
        o.customer_id,
        c.name as customer_name,
        o.order_total,
        o.ordered_at
    from orders o
    left join customers c
        on o.customer_id = c.customer_id
)

select * from orders_with_customer