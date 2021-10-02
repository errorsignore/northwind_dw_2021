{{ config(materialized='table') }}

with customers as(
    select 
    customer_sk,
    customer_id
    from {{ref('dim_customers')}}
)
, orders_with_sk as(
    select orders.order_id,
    customers.customer_id,
    orders.ship_region,
    orders.shipped_date,
    orders.ship_country,
    orders.ship_name, 
    orders.ship_city as cidade,
    orders.freight as frete,
    orders.required_date


    from {{ref('stg_orders')}} orders
    left join customers on customers.customer_id = orders.customer_id
)

select * from orders_with_sk