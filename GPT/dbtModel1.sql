{{  
  config(  
    materialized='incremental',  
    unique_key='order_id'  
  )  
}}  
  
with orders as (  
    select * from {{ source('dbt_semantic_layer_demo', 'fct_orders') }}  
),  
orders_with_customer as (  
    select  
        o.order_id,  
        o.customer_id,  
        c.name as customer_name,  
        o.order_total,  
        o.ordered_at  
    from orders o  
    left join {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c  
        on o.customer_id = c.customer_id  
    {% if is_incremental() %}  
        where o.ordered_at > (select max(ordered_at) from {{ this }})  
    {% endif %}  
)  
  
select * from orders_with_customer;