with source_data as(
    SELECT
    order_id,
    product_id,
    unit_price,
    quantity,
    discount


    from {{source('erpnorthwind', 'order_details')}}
)

select * from source_data
