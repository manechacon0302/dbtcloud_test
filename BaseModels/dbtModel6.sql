SELECT
FORMAT_TIMESTAMP('%Y-%m', o.ordered_at) AS order_month,
oi.product_id,
      SUM(oi.product_price) total_revenue,
RANK() OVER (PARTITION BY FORMAT_TIMESTAMP('%Y-%m', o.ordered_at) ORDER BY SUM(oi.product_price) DESC) as rank
FROM
`propellingtech-demo-customers.dbt_semantic_layer_demo.fct_orders` o,
`propellingtech-demo-customers.dbt_semantic_layer_demo.order_items` oi
WHERE o.order_id = oi.order_id
GROUP BY 1;
