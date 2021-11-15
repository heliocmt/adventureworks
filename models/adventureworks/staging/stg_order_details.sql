with
        source_data as (
            select
            salesorderid		
            , salesorderdetailid	
            , orderqty		
            , productid		
            , unitprice		
            , unitpricediscount	
            from {{ source('erp_adventureworks','order_details')}}
        )
        select * from source_data