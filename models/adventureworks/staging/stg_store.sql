with
        source_data as (
            select	
            businessentityid as storecustomerid
            , name as storename	
            from {{ source('erp_adventureworks','store')}}
        )
        select * from source_data