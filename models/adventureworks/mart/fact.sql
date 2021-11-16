{{ config(materialized='table') }}
with

    creditcard as (
        select 
        creditcard_fk
        , creditcardid
        , cardtype
        from {{ ref('dim_creditcard') }} as creditcard
    ), 

    order_details as (
        select	
        salesorderid	
        , product_fk
        , productid
        , product_name	
        , unitprice		
        , orderqty			
        from {{ ref('stg_order_details') }} as order_details
    ),

    customers as (
        select 
        customer_fk
        , customerid
        , personid
        , firstname
        , lastname 
        from {{ ref('dim_customers') }} as customers
    ),

    address1 as (
        select 
        address_fk
        , addressid	
        , addressline1			
        , city	
        , province
        , country
        , stateprovinceid
        , postalcode	
        from {{ ref('dim_address') }} as address1
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
        
    orders_with_sk as(     
        select 
        customerid
        , addressid
        , order_details.salesorderid	
        , order_details.product_fk
        , order_details.productid
        , order_details.product_name	
        , order_details.unitprice		
        , order_details.orderqty	
        , orderdate 
        , duedate 
        , shipdate 
        , subtotal 
        , taxamt 
        , freight 
        , totaldue
        , status
        , creditcardid 
        , shiptoaddressid 
        , shipmethodid 	
        from orders as orders_with_sk
        right join order_details on orders_with_sk.salesorderid = order_details.salesorderid
        ),

    final as (
        select 
        salesorderid
        , orderdate 
        , duedate 
        , shipdate 
        , subtotal 
        , taxamt 
        , freight 
        , totaldue
        , status
        , address1.addressid 
        , shiptoaddressid 
        , shipmethodid 			
        , creditcard.creditcard_fk  
        , creditcard.creditcardid
        , creditcard.cardtype
        , address1.address_fk
        , customerid
        , address1.addressline1			
        , address1.city	
        , address1.province
        , address1.country
        , product_fk
        , productid
        , product_name	
        , unitprice		
        , orderqty
        from orders_with_sk as final     
        left join creditcard on final.creditcardid = creditcard.creditcardid
        left join address1 on final.addressid = address1.addressid
        ),
        final2 as(
        select
        salesorderid
        , orderdate 
        , duedate 
        , shipdate 
        , subtotal 
        , taxamt 
        , freight 
        , totaldue
        , status
        , addressid 
        , shiptoaddressid 
        , shipmethodid 			
        , creditcard_fk  
        , creditcardid
        , cardtype
        , address_fk
        , addressline1			
        , city	
        , province
        , country
        , product_fk
        , productid
        , product_name	
        , unitprice		
        , orderqty
        , customers.customer_fk
        , customers.customerid	
        , customers.firstname	
        , customers.lastname
        , customers.personid
        from final as final2
        left join customers on final2.customerid = customers.customerid
        )

        select * from final2