-- Add the following source configuration to your `sources.yml` file:
-- 
-- sources:
--   - name: semantic_layer_demo
--     tables:
--       - name: fct_orders
--       - name: order_items

{{ config(
    materialized='table'
) }}

WITH order_data AS (
    SELECT
        o.order_id,
        DATE_TRUNC(o.ordered_at, MONTH) AS order_month,  -- Use DATE_TRUNC for optimized date handling
        oi.product_id,
        oi.product_price
    FROM
        {{ source('semantic_layer_demo', 'fct_orders') }} o
    JOIN
        {{ source('semantic_layer_demo', 'order_items') }} oi
    ON
        o.order_id = oi.order_id
),

aggregated_revenue AS (
    SELECT
        order_month,
        product_id,
        SUM(product_price) AS total_revenue
    FROM
        order_data
    GROUP BY
        order_month,
        product_id
)

SELECT
    FORMAT_TIMESTAMP('%Y-%m', order_month) AS order_month,
    product_id,
    total_revenue,
    RANK() OVER (
        PARTITION BY order_month
        ORDER BY total_revenue DESC
    ) AS rank

FROM
    aggregated_revenue

ORDER BY
    order_month,
    rank

-- Optimization notes:
-- 1. Replaced implicit join with explicit JOIN for clarity and performance.
-- 2. Used DATE_TRUNC to truncate timestamp to month for better engine optimization instead of FORMAT_TIMESTAMP in GROUP BY.
-- 3. Extracted initial join and filtering into a CTE to avoid repeated function calls.
-- 4. Grouped by both order_month and product_id correctly instead of only by order_month.
-- 5. Used source references per dbt best practices for managing table dependencies dynamically.
-- 6. Added ORDER BY at the end for predictable output.
