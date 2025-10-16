with sales as (
    select * from {{ ref('stg_orders') }}
),
sales_by_day as (
    select
        date_trunc('day', created_at) as day,
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
        (select avg(total_sales) from sales_by_day) as avg_sales_ref
    from sales_by_day s
)
select * from sales_with_avg;
