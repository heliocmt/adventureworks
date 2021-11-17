with
        source_data as (
            select
            row_number() over (order by productid) as product_sk
            , productid	
            , name as product_name
            , productnumber	
            from {{ source('erp_adventureworks','prooducts')}} as source_data
        )
        select * from source_data