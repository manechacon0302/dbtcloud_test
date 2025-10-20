{{  
  /*
  Add to your sources.yml under the appropriate source:
  
  sources:
    - name: semantic_layer_demo
      tables:
        - name: fct_orders
        - name: order_items
  */
}}

{{ config(
    materialized='table'
) }}

WITH orders AS (
    SELECT
        order_id,
        ordered_at
    FROM {{ source('semantic_layer_demo', 'fct_orders') }}
),

order_items AS (
    SELECT
        order_id,
        product_id,
        product_price
    FROM {{ source('semantic_layer_demo', 'order_items') }}
)

SELECT
    FORMAT_TIMESTAMP('%Y-%m', o.ordered_at) AS order_month,
    oi.product_id,
    SUM(oi.product_price) AS total_revenue,
    RANK() OVER (
        PARTITION BY FORMAT_TIMESTAMP('%Y-%m', o.ordered_at)
        ORDER BY SUM(oi.product_price) DESC
    ) AS rank
FROM
    orders o
JOIN
    order_items oi
    ON o.order_id = oi.order_id
GROUP BY
    order_month,
    oi.product_id

-- Optimizations applied:
-- 1. Replaced implicit join (comma join) with explicit JOIN for readability and potential optimization by query planner.
-- 2. Used CTEs with sources dynamically referenced via dbt source function for maintainability and environment flexibility.
-- 3. Grouped by aliased column `order_month` and product_id to avoid ambiguous grouping.
-- 4. Added inline comments and formatting for clarity.