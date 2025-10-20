/*
Add the following source configuration to your sources.yml file:

version: 2

sources:
  - name: dbt_semantic_layer_demo
    database: propellingtech-demo-customers
    schema: dbt_semantic_layer_demo
    tables:
      - name: dim_customers
      - name: fct_orders
      - name: order_items
*/

{{ config(
    materialized='table'
) }}

with base_orders as (
  -- Join customers, orders, and order_items once to avoid duplication and repeated scans
  select
    c.name,
    oi.is_food_item,
    oi.is_drink_item
  from {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c
  join {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o
    on c.customer_id = o.customer_id
  join {{ source('dbt_semantic_layer_demo', 'order_items') }} oi
    on o.order_id = oi.order_id
  -- No filters here to allow aggregation by flags below
),

aggregated_items as (
  select
    name,
    sum(case when is_food_item = 1 then 1 else 0 end) as food_item_count,
    sum(case when is_drink_item = 1 then 1 else 0 end) as drink_item_count
  from base_orders
  group by name
)

select
  name,
  food_item_count as item_count,
  'food' as item_type
from aggregated_items
where food_item_count > 0

union all

select
  name,
  drink_item_count as item_count,
  'drink' as item_type
from aggregated_items
where drink_item_count > 0

-- Optimization:
-- 1. Reduced triple join repeated twice into a single subquery (base_orders) to minimize scanning.
-- 2. Aggregated food and drink counts in one pass using conditional SUM to avoid multiple GROUP BY operations.
-- 3. Used UNION ALL instead of UNION because there can't be duplicates across food and drink item types, improving performance.
-- 4. Replaced fully qualified table names with dbt source references for better maintainability.
-- 5. Added filters in the final selects to exclude zero counts to reduce output size.