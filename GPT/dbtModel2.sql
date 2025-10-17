{{ config(
    materialized='incremental',
    unique_key='day',
    partition_by={"field": "day", "data_type": "date"},
    cluster_by=["day"]
) }}

with sales as (
    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
        where created_at >= (select max(day) from {{ this }})
    {% endif %}
),
sales_by_day as (
    select
        date(created_at) as day,
        sum(total_amount) as total_sales
    from sales
    group by 1
),
sales_with_avg as (
    select
        s.day,
        s.total_sales,
        avg(s.total_sales) over () as avg_sales_ref
    from sales_by_day s
)

select * from sales_with_avg;