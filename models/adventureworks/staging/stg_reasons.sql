with
        source_data as (
            select
            salesreasonid		
            , name as reason		
            , reasontype
            from {{ source('erp_adventureworks','reasons')}}
        )
        select * from source_data