{{ config(
    materialized='incremental',
    partition_by={"field": "ordered_at", "data_type": "date"},
    unique_key='product_id'
) }}

select
    distinct oi.product_id,
    oi.product_price,
    date(o.ordered_at) as ordered_at
from
    {{ source('dbt_semantic_layer_demo', 'order_items') }} oi
right join
    {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
    on o.order_id = oi.order_id
where
    date(o.ordered_at) >= date_sub(current_date(), interval 90 day)

{% if is_incremental() %}
    and date(o.ordered_at) > (select max(ordered_at) from {{ this }})
{% endif %}