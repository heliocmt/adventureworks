with
        source_data as (
            select
            , customerid	
            , personid	
            , territoryid
            from {{ source('erp_adventureworks','customers')}}
        )
        select * from source_data