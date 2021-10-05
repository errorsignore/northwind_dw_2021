{{ config(materialized='table') }}

with products as (
    select *
    from {{ref('stg_products')}}
),

        categories as (
            select *
            from {{ref('stg_categories')}}
        ),

    transformed as(
        select


            row_number() over (order by products.product_id) as product_sk, -- auto-incremental surrogate key
            products.product_id,
            products.product_name,
            products.supplier_id,
            products.category_id,
            products.quantity_per_unit,
            products.unit_price,
            products.units_in_stock,
            products.units_on_order,
            products.reorder_level,
            products.discontinued,
            categories.category_name,
            categories.description

        from products
        left join categories on categories.category_id = products.category_id

    )

    select * from transformed