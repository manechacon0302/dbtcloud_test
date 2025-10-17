```sql
{{ 
  config(
    materialized = "incremental",
    unique_key = "order_id",
    incremental_strategy = "merge",
    tags = ["sales", "orders"],
    on_schema_change = "sync_all_columns",
    meta = {
      "owner": "analytics_engineering",
      "description": "Incremental model optimized for recent orders with customer join and aggregated metrics"
    }
  ) 
}}

with filtered_orders as (
  select
    cast(order_id as integer) as order_id,               -- cast once for optimization and clarity
    cast(customer_id as integer) as customer_id,
    cast(order_date as date) as order_date,               -- date precision sufficient, avoid timestamp
    cast(order_status as varchar(20)) as order_status,    -- sized varchar to minimize storage
    cast(total_amount as numeric(10,2)) as total_amount
  from {{ ref('raw_orders') }}
  {% if is_incremental() %}
  where order_date > (
    select coalesce(max(order_date), '1900-01-01') from {{ this }}
  )
  {% endif %}
),

customer_info as (
  select
    cast(customer_id as integer) as customer_id,
    cast(customer_name as varchar(100)) as customer_name,
    cast(customer_segment as varchar(50)) as customer_segment
  from {{ ref('dim_customers') }}
),

orders_with_customer as (
  select
    o.order_id,
    o.customer_id,
    c.customer_name,
    c.customer_segment,
    o.order_date,
    o.order_status,
    o.total_amount,
    
    -- Using window function for rank to identify latest order per customer if needed elsewhere (example)
    -- Removed as not needed here: keep only necessary columns and joins
    
    -- Add any necessary metric computations inline if needed

  from filtered_orders o
  left join customer_info c on o.customer_id = c.customer_id       -- LEFT JOIN assuming orders might exist without customer info
)

select
  order_id,
  customer_id,
  customer_name,
  customer_segment,
  order_date,
  order_status,
  total_amount
from orders_with_customer
```