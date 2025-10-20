/*
Add to your `sources.yml` file:

sources:
  - name: semantic_layer_demo
    database: propellingtech-demo-customers
    schema: dbt_semantic_layer_demo
    tables:
      - name: dim_customers
      - name: fct_orders
      - name: order_items
*/

{{ 
  config(
    materialized='table'
  ) 
}}

with high_value_customers as (
    -- Compute customers with total orders > 500 using GROUP BY instead of HAVING alone for better performance
    select
        customer_id
    from {{ source('semantic_layer_demo', 'fct_orders') }}
    group by customer_id
    having sum(order_total) > 500
),

orders_customers as (
    -- Join orders and customers filtered by high value customers first to reduce dataset early
    select
        o.order_id,
        o.customer_id
    from {{ source('semantic_layer_demo', 'fct_orders') }} o
    join high_value_customers hvc
        on o.customer_id = hvc.customer_id
),

order_items_filtered as (
    -- Filter order items with product_price > 20 early to reduce join cost
    select
        order_id,
        product_id,
        product_price
    from {{ source('semantic_layer_demo', 'order_items') }}
    where product_price > 20
)

select
    c.name,
    oi.product_id,
    oi.product_price
from orders_customers oc
join {{ source('semantic_layer_demo', 'dim_customers') }} c
    on oc.customer_id = c.customer_id
join order_items_filtered oi
    on oc.order_id = oi.order_id

-- Optimizations applied:
-- 1. Added CTE `high_value_customers` with GROUP BY + HAVING to optimize filtering customers by order_total.
-- 2. Filtered fct_orders by high value customers before joining to reduce join data size.
-- 3. Filtered order_items by product_price > 20 in a CTE early.
-- 4. Corrected join between order_items and orders_customers on order_id (instead of incorrect c.customer_id = oi.order_id).
-- 5. Used dbt `source()` for all referenced tables for better maintainability and config.
-- 6. Clean, well formatted and no extra fields added.
