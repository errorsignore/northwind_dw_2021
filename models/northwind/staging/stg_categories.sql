with source_data as(
    SELECT
    category_id,
    category_name,
    description

    from {{source('erpnorthwind', 'categories')}}
)

select * from source_data
