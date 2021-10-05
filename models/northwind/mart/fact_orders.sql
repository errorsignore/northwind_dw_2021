{{ config(materialized='table') }}

with customers as(
    select 
    customer_sk,
    customer_id
    from {{ref('dim_customers')}}
), products as(
    select 
    product_sk,
    product_id
    from {{ref('dim_products')}}
), employees as(
    select 
    employee_sk,
    employee_id
    from {{ref('dim_employees')}}
), order_details as(
    select *
    from {{ref('stg_order_details')}}
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
    e.employee_id,
    p.product_sk,
    p.product_id,
    sh.shipper_sk,
    sh.shipper_id,
    sp.supplier_sk,
    sp.supplier_id,
    o.ship_region,
    o.shipped_date,
    o.ship_country,
    o.ship_name, 
    o.ship_city as cidade,
    o.freight as frete,
    o.required_date,
    od.product_id,
    od.unit_price,
    od.quantity,
    od.discount


    from {{ref('stg_orders')}} o
    left join order_details od on od.order_id = o.order_id
    left join customers c on c.customer_id = o.customer_id
    left join employees e on e.employee_id = o.employee_id
    left join products p on p.product_id = od.product_id
    left join shippers sh on sh.shipper_id = o.shipper_id
    left join suppliers sp on sp.supplier_id = p.supplier_id
)

select * from orders_with_sk