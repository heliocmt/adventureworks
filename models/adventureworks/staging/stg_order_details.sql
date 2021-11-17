with     
        order_details as (
        select	
        salesorderid
        , productid		
        , unitprice		
        , orderqty		
        , salesorderdetailid	
        , unitpricediscount		
        from {{ source('erp_adventureworks','order_details') }}
    )
    select order_details