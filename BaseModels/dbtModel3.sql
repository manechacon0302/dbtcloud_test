SELECT
  DISTINCT oi.product_id,
  oi.product_price,
  o.ordered_at
FROM
  `propellingtech-demo-customers.dbt_semantic_layer_demo.fct_orders` o
RIGHT JOIN
  `propellingtech-demo-customers.dbt_semantic_layer_demo.order_items` oi ON o.order_id = oi.order_id
WHERE
  DATE(o.ordered_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY);
