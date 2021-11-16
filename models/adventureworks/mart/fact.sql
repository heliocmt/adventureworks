{{ config(materialized='table') }}

with
    order_details as (
        select	
        salesorderid
        , productid		
        , unitprice		
        , orderqty		
        , salesorderdetailid	
        , unitpricediscount		
        from {{ ref('stg_order_details') }} as order_details
    ),
     orders as (
        select        
        salesorderid
        , customerid 
        , orderdate 
        , duedate 
        , shipdate 
        , subtotal 
        , taxamt 
        , freight 
        , totaldue
        , territoryid 
        , status
        , billtoaddressid as addressid 
        , shiptoaddressid 
        , shipmethodid 
        , creditcardid	
        from {{ ref('stg_orders') }} as orders
    ),
    fact as(
        select
        order_details.salesorderid
        , customerid 
        , orderdate 
        , duedate 
        , shipdate 
        , subtotal 
        , taxamt 
        , freight 
        , totaldue
        , territoryid 
        , status
        , addressid 
        , shiptoaddressid 
        , shipmethodid 
        , creditcardid	
        , order_details.productid		
        , order_details.unitprice		
        , order_details.orderqty		
        , order_details.salesorderdetailid	
        , order_details.unitpricediscount	
        from orders as fact
        left join order_details on fact.salesorderid = order_details.salesorderid
    )
    select * from fact
    order by salesorderid