-- Add these lines to your sources.yml file:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     schema: dbt_semantic_layer_demo
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
    -- Using dbt source reference for proper dependency management
    select 
        order_id,
        customer_id,
        order_total,
        ordered_at
    from {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
),

customers as (
    -- Using dbt source reference and selecting only required fields to reduce memory footprint
    select 
        customer_id,
        name
    from {{ source('dbt_semantic_layer_demo', 'dim_customers') }}
),

orders_with_customer as (
    -- Replaced correlated subquery with proper JOIN for better performance
    -- BigQuery executes JOINs more efficiently than row-by-row subqueries
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