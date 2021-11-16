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
        salesorderid
        , customers.customer_fk
        , address1.address_fk
        , customers.customerid	
        , customers.firstname	
        , customers.lastname
        , customers.personid
        , orderdate 
        , duedate 
        , shipdate 
        , subtotal 
        , taxamt 
        , freight 
        , totaldue
        , status
        , creditcardid
        , address1.addressid 
        , shiptoaddressid 
        , shipmethodid 	
        , address1.addressline1			
        , address1.city	
        , address1.province
        , address1.country
        from orders as orders_with_sk
        right join customers on orders_with_sk.customerid = customers.customerid
        left join address1 on orders_with_sk.addressid = address1.addressid
        ),

    final as (
        select 
        customer_fk
        , address_fk
        , customerid	
        , firstname	
        , lastname
        , personid
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
        , addressline1			
        , city	
        , province
        , country
        , creditcard.creditcard_fk  
        , creditcard.creditcardid
        , creditcard.cardtype
        , order_details.salesorderid	
        , order_details.product_fk
        , order_details.productid
        , order_details.product_name	
        , order_details.unitprice		
        , order_details.orderqty		
        from orders_with_sk as final     
        left join creditcard on final.creditcardid = creditcard.creditcardid
        left join order_details on final.salesorderid = order_details.salesorderid
        )
        select * from final