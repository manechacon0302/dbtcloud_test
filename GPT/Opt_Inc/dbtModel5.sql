{{  
  config(  
    materialized='incremental',  
    unique_key='product_id',  
    partition_by={  
      "field": "order_date",  
      "data_type": "date"  
    }  
  )  
}}  
  
WITH high_value_customers AS (  
    SELECT  
        customer_id  
    FROM {{ source('dbt_semantic_layer_demo', 'fct_orders') }}  
    GROUP BY customer_id  
    HAVING SUM(order_total) > 500  
),  
  
filtered_orders AS (  
    SELECT  
        o.order_id,  
        o.customer_id,  
        o.order_date  
    FROM {{ source('dbt_semantic_layer_demo', 'fct_orders') }} o  
    WHERE o.customer_id IN (SELECT customer_id FROM high_value_customers)  
)  
  
SELECT  
    c.name,  
    oi.product_id,  
    oi.product_price  
FROM {{ source('dbt_semantic_layer_demo', 'dim_customers') }} c  
JOIN filtered_orders o ON c.customer_id = o.customer_id  
JOIN {{ source('dbt_semantic_layer_demo', 'order_items') }} oi ON o.order_id = oi.order_id  
WHERE oi.product_price > 20  
  
{% if is_incremental() %}  
  AND o.order_date > (SELECT COALESCE(MAX(order_date), '1900-01-01') FROM {{ this }})  
{% endif %}