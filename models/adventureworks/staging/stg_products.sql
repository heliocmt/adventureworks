with
        source_data as (
            select
            productid	
            , name	
            , productnumber	
            from {{ source('erp_adventureworks','orders')}}
        )
        select * from source_data