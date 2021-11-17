with
        customer as (
            select
            row_number() over (order by customerid) as customer_fk
            , customerid	
            , personid
            , storeid	
            from {{ source('erp_adventureworks','customers')}} as customer
        )
        select * from customer
    