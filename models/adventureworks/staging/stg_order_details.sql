with
        source_data as (
            select
            salesorderid		
            , salesorderdetailid	
            , carriertrackingnumber	
            , orderqty		
            , productid		
            , unitprice		
            , unitpricediscount		
            , rowguid		
            , modifieddate
            from {{ source('erp_adventureworks','fact_orders_details')}}
        )
        select * from source_data