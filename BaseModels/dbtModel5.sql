SELECT c.name oi.product_id, oi.product_price FROM `propellingtech-demo-customers.dbt_semantic_layer_demo.dim_customers` c JOIN `propellingtech-demo-customers.dbt_semantic_layer_demo.fct_orders` o ON c.customer_id = o.customer_id
JOIN `propellingtech-demo-customers.dbt_semantic_layer_demo.order_items` oi ON c.customer_id = oi.order_id
WHERE oi.product_price > 20
AND c.customer_id IN (SELECT customer_id FROM `propellingtech-demo-customers.dbt_semantic_layer_demo.fct_orders` HAVING SUM(order_total) > 500);
