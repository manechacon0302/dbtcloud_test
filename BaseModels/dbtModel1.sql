with orders as (
    select * from propellingtech-demo-customers.dbt_semantic_layer_demo.fct_orders
),
customers as (
    select * from propellingtech-demo-customers.dbt_semantic_layer_demo.dim_customers
),
orders_with_customer as (
    select
        o.order_id,
        o.customer_id,
        (select c.name from propellingtech-demo-customers.dbt_semantic_layer_demo.dim_customers c where c.customer_id = o.customer_id) as customer_name,
        o.order_total,
        o.ordered_at
    from orders o
)
select * from orders_with_customer;



