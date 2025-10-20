-- Add these source definitions to your schema.yml file:
-- sources:
--   - name: dbt_semantic_layer_demo
--     database: propellingtech-demo-customers
--     tables:
--       - name: dim_customers
--       - name: fct_orders
--       - name: order_items

{{ config(
    materialized='table',
    partition_by={
      "field": "customer_id",
      "data_type": "int64",
      "range": {
        "start": 0,
        "end": 100000000,
        "interval": 1000
      }
    }
) }}

-- Optimized query with corrected syntax and performance improvements
WITH high_value_customers AS (
  -- Pre-filter customers with total orders > 500 to reduce join size
  -- Using CTE instead of correlated subquery for better performance
  SELECT 
    customer_id
  FROM {{ source('dbt_semantic_layer_demo', 'fct_orders') }}
  GROUP BY customer_id
  HAVING SUM(order_total) > 500
),

filtered_order_items AS (
  -- Pre-filter order items by price to reduce data volume early
  SELECT 
    order_id,
    product_id,
    product_price
  FROM {{ source('dbt_semantic_layer_demo', 'order_items') }}
  WHERE product_price > 20
)

SELECT 
  c.name,
  oi.product_id,
  oi.product_price
FROM {{ source('dbt_semantic_layer_demo', 'dim_customers') }} AS c
-- Use INNER JOIN with pre-filtered high value customers to reduce join cardinality
INNER JOIN high_value_customers AS hvc
  ON c.customer_id = hvc.customer_id
-- Join with orders table
INNER JOIN {{ source('dbt_semantic_layer_demo', 'fct_orders') }} AS o
  ON c.customer_id = o.customer_id
-- Fixed join condition: order_items should join on order_id, not customer_id
INNER JOIN filtered_order_items AS oi
  ON o.order_id = oi.order_id