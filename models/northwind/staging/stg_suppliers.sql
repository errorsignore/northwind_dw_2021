with source_data as(
    SELECT
    supplier_id,
    company_name,
    contact_name,
    contact_title,
    address,
    city,
    region,
    postal_code,
    country,
    phone,
    fax,
    homepage



    from {{source('erpnorthwind', 'suppliers')}}
)

select * from source_data
