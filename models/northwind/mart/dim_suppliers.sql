{{ config(materialized='table') }}

with supplier as (
    select *
    from {{ref('stg_suppliers')}}

)

    , transformed as(
        select


            row_number() over (order by s.supplier_id) as supplier_sk, -- auto-incremental surrogate key
            sp.supplier_id,
            sp.company_name,
            sp.contact_name,
            sp.contact_title,
            sp.address,
            sp.city,
            sp.region,
            sp.postal_code,
            sp.country,
            sp.phone,
            sp.fax,
            sp.homepage
        from supplier sp

    ) 

    select * from transformed