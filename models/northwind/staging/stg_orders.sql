with 
    source_data as (
        select order_id, employee_id, order_date, customer_id, ship_region, shipped_date, ship_country,
        freight, ship_via as shipper_id, ship_address, required_date, ship_name, ship_city

        from {{source('erpnorthwind', 'public_orders')}}
    )

select * from source_data