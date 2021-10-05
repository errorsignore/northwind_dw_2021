{{ config(materialized='table') }}

with customers as(
    select 
    customer_sk,
    customer_id
    from {{ref('dim_customers')}}
), produtos as(
    select 
    product_sk,
    product_id,
    supplier_id
    from {{ref('dim_products')}}
), employees as(
    select 
    employee_sk,
    employee_id
    from {{ref('dim_employees')}}
), shippers as(
    select shipper_sk,
    shipper_id
    from {{ref('dim_shippers')}}
), suppliers as(
    select supplier_sk,
    supplier_id
    from {{ref('dim_suppliers')}}
)

, orders_with_sk as(
    select o.order_id,
    c.customer_sk,
    c.customer_id,
    e.employee_sk,
    p.product_sk,
    sh.shipper_sk,
    sp.supplier_sk,
    o.ship_region,
    o.shipped_date,
    o.ship_country,
    o.ship_name, 
    o.ship_city as cidade,
    o.freight as frete,
    o.required_date,
    od.unit_price,
    od.quantity,
    od.discount


    from {{ref('stg_orders')}} o
    left join {{ref ('stg_order_details')}} od on od.order_id = o.order_id
    left join customers c on c.customer_id = o.customer_id
    left join employees e on e.employee_id = o.employee_id
    left join produtos p on p.product_id = od.product_id
    left join shippers sh on sh.shipper_id = o.shipper_id
    left join suppliers sp on sp.supplier_id = p.supplier_id
)

select * from orders_with_sk