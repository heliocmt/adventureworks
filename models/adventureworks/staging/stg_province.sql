with
        province1 as (
            select
            name as province
            , territoryid		
            , countryregioncode
            , stateprovincecode
            , stateprovinceid
            from {{ source('erp_adventureworks','stateprovince')}} 
        )
        select * from province1
