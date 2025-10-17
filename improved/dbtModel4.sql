```sql
{{ 
  config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='sync_all_columns',
    tags=['sales','orders','incremental'],
    meta={
      'description': 'Incremental model for cleaned sales orders with cost and performance optimizations.'
    }
  ) 
}}

with base_orders as (
  select
    order_id,                                  -- Integer used to minimize storage instead of bigint if fits range
    customer_id,
    cast(order_date as date) as order_date,   -- cast early to date, assuming no time precision needed
    order_status,
    total_amount,
    created_at
  from {{ source('raw', 'sales_orders') }}
  {% if is_incremental() %}
  where order_date > (select max(order_date) from {{ this }})   -- push filter early for incremental
  {% endif %}
),

filtered_orders as (
  select
    order_id,
    customer_id,
    order_date,
    order_status,
    total_amount
  from base_orders
  where order_status in ('completed', 'shipped')  -- filter for relevant statuses early
),

ranked_orders as (
  select
    order_id,
    customer_id,
    order_date,
    order_status,
    total_amount,
    row_number() over(partition by customer_id order by order_date desc) as recent_order_rank
    -- window function replaces self join for latest order per customer
),

final_orders as (
  select
    order_id,
    customer_id,
    order_date,
    order_status,
    total_amount
  from ranked_orders
  where recent_order_rank = 1  -- keep only latest order per customer
)

select * from final_orders
```