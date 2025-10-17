with products as (
    select distinct
        p.product_id,
        p.product_name,
        c.category_name
    from {{ source('shop', 'products') }} p
    left join {{ source('shop', 'categories') }} c
        on p.category_id = c.category_id
    left join {{ source('shop', 'categories') }} c2
        on p.category_id = c2.category_id
)
select * from products;
