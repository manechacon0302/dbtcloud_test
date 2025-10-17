```sql
{{ 
  config(
    materialized = "incremental",      -- incremental for cost efficiency and freshness
    unique_key = "order_id",           -- unique key for incremental load
    on_schema_change = "sync_all_columns",  
    tags = ["orders", "finance"],      -- tags for model organization
    incremental_strategy = "insert_overwrite",  -- overwrite partitions on update
    partition_by = {field="order_date", data_type="date"},  -- partitioning for pruning
    cluster_by = ["customer_id"]       -- clustering to optimize joins/filters
  )
}}

with filtered_source as (
    select
        cast(order_id as integer) as order_id,
        cast(customer_id as integer) as customer_id,
        cast(order_date as date) as order_date,
        cast(order_status as varchar(20)) as order_status,
        cast(total_amount as numeric(12,2)) as total_amount
    from {{ source('raw_db', 'orders') }}
    {% if is_incremental() %}
      where order_date > (select max(order_date) from {{ this }})  -- push filters early 
    {% endif %}
),

-- Use window function to get latest status per order_id if there's multiple statuses (example of replacing subquery)
latest_order_status as (
    select 
        order_id,
        customer_id,
        order_date,
        order_status,
        total_amount,
        row_number() over (partition by order_id order by order_date desc) as rn
    from filtered_source
)

select
    order_id,
    customer_id,
    order_date,
    order_status,
    total_amount
from latest_order_status
where rn = 1  -- get latest record per order_id

```