with
        source_data as (
            select
            productid	
            , name	
            , productnumber	
            from {{ source('erp_adventureworks','prooducts')}}
        )
        select * from source_data