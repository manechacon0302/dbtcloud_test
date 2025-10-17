```sql
{{ 
  config(
    materialized='incremental',              -- Incremental for performance on large datasets
    unique_key='user_id',                    -- Unique key for incremental merge
    on_schema_change='sync_all_columns',    -- Sync schema changes safely
    tags=['user_activity', 'incremental'],  -- Model organization
    meta={
      'description': 'Incremental model summarizing user activity with optimized joins and partition pruning'
    }
  ) 
}}

with source_filtered as (
  select 
    user_id::int,                           -- Cast early to integer for performance
    activity_type::varchar(20),             -- Limit varchar size appropriately
    activity_date::date                      -- Use date type if time is not required
  from {{ ref('user_activity_raw') }}
  where activity_date >= dateadd(day, -30, current_date)  -- Push date filter early for partition pruning
),

user_profiles as (
  select 
    user_id::int,
    user_name::varchar(100),                -- Limit varchar size
    signup_date::date
  from {{ ref('user_profiles') }}
),

-- Use window function instead of self-join for last activity
last_activity as (
  select 
    user_id,
    activity_date,
    row_number() over (partition by user_id order by activity_date desc) as rn
  from source_filtered
),

final as (
  select
    up.user_id,
    up.user_name,
    up.signup_date,
    count(sa.activity_type) as total_activities,
    max(la.activity_date) as last_activity_date
  from user_profiles up
  left join source_filtered sa 
    on up.user_id = sa.user_id
  left join last_activity la
    on up.user_id = la.user_id and la.rn = 1    -- Join only last activity per user
  group by 1, 2, 3
)

select * from final

{% if is_incremental() %}
  where user_id not in (
    select user_id from {{ this }}
  )
{% endif %}
```