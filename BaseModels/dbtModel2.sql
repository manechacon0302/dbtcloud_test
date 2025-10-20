SELECT
  (
    SELECT
      c.name
    FROM
      `propellingtech-demo-customers.dbt_semantic_layer_demo.dim_customers` c
    WHERE
      c.customer_id = o.customer_id
  ) AS customer_name,
  SUM(o.order_total) AS total_spent
FROM
  (
    SELECT
      *
    FROM
      `propellingtech-demo-customers.dbt_semantic_layer_demo.fct_orders`
  ) AS o
GROUP BY
  customer_name
ORDER BY
  total_spent DESC;
