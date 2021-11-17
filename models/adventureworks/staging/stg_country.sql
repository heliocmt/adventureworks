with
        stg_country as (
            select
            countryregioncode
            , name as country
            from {{ source('erp_adventureworks','countryregion')}} 
            )
        select * from stg_country