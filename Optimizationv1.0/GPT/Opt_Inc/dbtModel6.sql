{{ config(
    materialized='incremental',
    unique_key=['order_month', 'product_id'],
    incremental_strategy='merge'
) }}

with base as (
    select
        format_timestamp('%Y-%m', o.ordered_at) as order_month,
        oi.product_id,
        oi.product_price
    from {{ ref('fct_orders') }} o
    join {{ ref('order_items') }} oi on o.order_id = oi.order_id
    {% if is_incremental() %}
      where o.ordered_at >= (select max(ordered_at) from {{ this }})
    {% endif %}
)

select
    order_month,
    product_id,
    sum(product_price) as total_revenue,
    rank() over (
        partition by order_month
        order by sum(product_price) desc
    ) as rank
from base
group by
    order_month,
    product_id