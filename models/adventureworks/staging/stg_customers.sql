with
        customers as (
            select
            row_number() over (order by customerid) as customer_sk
            customerid	
            personid	
            storeid	
            territoryid
            from {{ source('erp_adventureworks','customers')}} 
        ),
        select * from customers
    