with     
        source_data as (
        select	
        salesorderid
        , productid		
        , unitprice		
        , orderqty		
        , salesorderdetailid	
        , unitpricediscount		
        from {{ source('erp_adventureworks','order_details') }} as source_data
    )
    select * from source_data