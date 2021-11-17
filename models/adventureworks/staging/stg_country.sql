with
        country as (
            select
            countryregioncode
            , name as country
            from {{ source('erp_adventureworks','countryregion')}} 
            )
        select * from country