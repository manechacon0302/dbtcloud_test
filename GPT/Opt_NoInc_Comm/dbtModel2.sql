{{ config(
    materialized='table'
) }}

-- Add to your sources.yml if not already present:
-- sources:
--   - name: dbt_semantic_layer_demo
--     tables:
--       - name: dim_customers
--       - name: fct_orders

with customers as (
    select
        customer_id,
        name as customer_name
    from 
        {{ source('dbt_semantic_layer_demo', 'dim_customers') }}
),

orders as (
    select
        customer_id,
        order_total
    from 
        {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
)

/* 
  Optimizations applied:
  1. Removed the correlated subquery in the SELECT clause - replaced by an explicit join which is more efficient.
  2. Removed unnecessary subquery wrapping 'fct_orders' as it was redundant.
  3. Used dbt's source function for maintainability and dynamic references.
  4. Selected only needed columns instead of SELECT * to reduce data scanned.
  5. Added materialized='table' configuration for performance (adjust as needed).
*/

select
    c.customer_name,
    sum(o.order_total) as total_spent
from
    orders o
    inner join customers c
        on o.customer_id = c.customer_id
group by
    c.customer_name
order by
    total_spent desc
;