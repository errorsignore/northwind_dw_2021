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
    select o.order_id as id_do_pedido,
    o.order_date as data_do_pedido,
    o.shipped_date as data_de_envio,
    o.required_date as prazo_de_envio,
    c.customer_sk,
    c.customer_id,
    e.employee_sk,
    p.product_sk,
    sh.shipper_sk,
    sp.supplier_sk,
    o.ship_country as pais,
    o.ship_city as cidade,
    o.freight as frete,
    od.unit_price as preco_unitario,
    od.quantity as quantidade,
    od.discount as disconto


    from {{ref('stg_orders')}} o
    left join {{ref ('stg_order_details')}} od on od.order_id = o.order_id
    left join customers c on c.customer_id = o.customer_id
    left join employees e on e.employee_id = o.employee_id
    left join produtos p on p.product_id = od.product_id
    left join shippers sh on sh.shipper_id = o.shipper_id
    left join suppliers sp on sp.supplier_id = p.supplier_id
)

select * from orders_with_sk