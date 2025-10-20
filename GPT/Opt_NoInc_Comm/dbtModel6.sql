/*
Add the following to your sources.yml under appropriate source name (e.g., semantic_layer_demo):

sources:
  - name: semantic_layer_demo
    tables:
      - name: fct_orders
      - name: order_items
*/

{{ config(
    materialized='table'
) }}

WITH order_data AS (
    SELECT
        -- Format order date to 'YYYY-MM' for month aggregation
        FORMAT_TIMESTAMP('%Y-%m', o.ordered_at) AS order_month,
        oi.product_id,
        oi.product_price
    FROM
        {{ source('semantic_layer_demo', 'fct_orders') }} o
    INNER JOIN
        {{ source('semantic_layer_demo', 'order_items') }} oi
    ON
        o.order_id = oi.order_id
)

SELECT
    order_month,
    product_id,
    SUM(product_price) AS total_revenue,
    -- Use SUM window function inside RANK to avoid recomputing aggregation
    RANK() OVER (
        PARTITION BY order_month
        ORDER BY SUM(product_price) DESC
    ) AS rank
FROM
    order_data
GROUP BY
    order_month,
    product_id

-- Optimizations:
-- 1. Replaced implicit JOIN with explicit INNER JOIN for clarity and better planning.
-- 2. Removed repeated FORMAT_TIMESTAMP calls by using a CTE and aliasing order_month once.
-- 3. Included product_id in GROUP BY as required (it was missing originally causing a syntax error).
-- 4. Used source() macros for dynamic referencing per dbt best practices.
-- 5. Kept only the fields from original query, no extras.
-- 6. Used proper indentation and spacing for readability.
