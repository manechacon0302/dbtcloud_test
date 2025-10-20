{{ config(
    materialized='table'
) }}

/*
Add the following to your sources.yml for dynamic references:

sources:
  - name: propellingtech_demo_customers
    tables:
      - name: fct_orders
      - name: order_items
*/

with recent_orders as (
    select
        order_id,
        ordered_at
    from
        {{ source('propellingtech_demo_customers', 'fct_orders') }}
    where
        ordered_at >= date_sub(current_date(), interval 90 day)  -- Filter early to reduce join size and avoid applying DATE() on column for better performance
)

select
    distinct
    oi.product_id,
    oi.product_price,
    ro.ordered_at
from
    {{ source('propellingtech_demo_customers', 'order_items') }} oi
right join
    recent_orders ro
    on ro.order_id = oi.order_id
where
    ro.ordered_at is not null  -- Ensure we exclude rows without matching orders (since originally filtered on ordered_at)