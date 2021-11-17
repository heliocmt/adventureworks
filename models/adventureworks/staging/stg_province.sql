with
        stg_province as (
            select
            name as province
            , territoryid		
            , countryregioncode
            , stateprovincecode
            , stateprovinceid
            from {{ source('erp_adventureworks','stateprovince')}} 
        )
        select * from stg_province
