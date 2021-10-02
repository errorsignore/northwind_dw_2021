with 
    source_data as (
        select order_id, employee_id, order_date, custormer_id, ship_region, shipped_date, ship_country,
        freights, ship_via as shipper_id, ship_adress, required_date

        from {{source('erpnorthwind', 'orders')}}
    )

select * from source_data