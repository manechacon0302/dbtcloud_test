WITH food_orders AS (
  SELECT
    c.name,
    COUNT(oi.order_item_id) AS item_count
  FROM
    `propellingtech-demo-customers.dbt_semantic_layer_demo.dim_customers` c
    JOIN `propellingtech-demo-customers.dbt_semantic_layer_demo.fct_orders` o ON c.customer_id = o.customer_id
    JOIN `propellingtech-demo-customers.dbt_semantic_layer_demo.order_items` oi ON o.order_id = oi.order_id
  WHERE
    oi.is_food_item = 1
  GROUP BY
    c.name
),
drink_orders AS (
  SELECT
    c.name,
    COUNT(oi.order_item_id) AS item_count
  FROM
    `propellingtech-demo-customers.dbt_semantic_layer_demo.dim_customers` c
    JOIN `propellingtech-demo-customers.dbt_semantic_layer_demo.fct_orders` o ON c.customer_id = o.customer_id
    JOIN `propellingtech-demo-customers.dbt_semantic_layer_demo.order_items` oi ON o.order_id = oi.order_id
  WHERE
    oi.is_drink_item = 1
  GROUP BY
    c.name
)
SELECT
  name,
  item_count,
  'food' AS item_type
FROM
  food_orders
UNION
SELECT
  name,
  item_count,
  'drink' AS item_type
FROM
  drink_orders;