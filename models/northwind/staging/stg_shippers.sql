with source_data as(
    SELECT
    shipper_id,
    company_name,
    phone



    from {{source('erpnorthwind', 'shippers')}}
)

select * from source_data
