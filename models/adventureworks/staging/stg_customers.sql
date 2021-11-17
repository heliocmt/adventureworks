with
        customers as (
            select
            row_number() over (order by customerid) as customer_sf
            , customerid	
            , personid	
            , storeid	
            from {{ source('erp_adventureworks','customers')}} 
        )
        select * from customers
    