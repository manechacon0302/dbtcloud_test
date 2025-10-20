{{ config(
    materialized='incremental',
    unique_key=['name', 'item_type'],
    partition_by={"field": "order_date", "data_type": "date"}
) }}

with base_data as (
    select
        c.customer_id,
        c.name,
        o.order_id,
        o.order_date,
        oi.order_item_id,
        oi.is_food_item,
        oi.is_drink_item
    from {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
    join {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
        on c.customer_id = o.customer_id
    join {{ source('dbt_semantic_layer_demo', 'order_items') }} oi
        on o.order_id = oi.order_id
    where oi.is_food_item = 1 or oi.is_drink_item = 1
),

food_orders as (
    select
        name,
        count(order_item_id) as item_count,
        date(order_date) as order_date
    from base_data
    where is_food_item = 1
    group by name, order_date
),

drink_orders as (
    select
        name,
        count(order_item_id) as item_count,
        date(order_date) as order_date
    from base_data
    where is_drink_item = 1
    group by name, order_date
)

select
    name,
    item_count,
    'food' as item_type,
    order_date
from food_orders

union all

select
    name,
    item_count,
    'drink' as item_type,
    order_date
from drink_orders

{% if is_incremental() %}
where order_date >= (select coalesce(max(order_date), '1970-01-01') from {{ this }})
{% endif %}