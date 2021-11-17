with
        address1 as (
            select
            row_number() over (order by addressid) as address_fk
            , addressid	
            , addressline1			
            , city	
            , postalcode		
            from {{ source('erp_adventureworks','address')}} as address1
        )
        select * from address1
