{{ config(
    materialized='incremental',
    unique_key='day',
    on_schema_change='fail',
    partition_by={
        'field': 'day',
        'data_type': 'date',
        'granularity': 'day'
    },
    cluster_by=['day'],
    incremental_strategy='merge'
) }}

with sales as (
    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
    where date_trunc(created_at, day) > (select max(day) from {{ this }})
    {% endif %}
),
sales_by_day as (
    select
        date_trunc(created_at, day) as day,
        sum(total_amount) as total_sales
    from sales
    group by 1
),
avg_sales as (
    select
        avg(total_sales) as avg_daily_sales
    from sales_by_day
),
sales_with_avg as (
    select
        s.day,
        s.total_sales,
        a.avg_daily_sales as avg_sales_ref
    from sales_by_day s
    cross join avg_sales a
)
select * from sales_with_avg