{{ config(materialized='table') }}
with

    creditcard as (
        select 
        creditcard_fk
        , creditcardid
        , cardtype
        from {{ ref('dim_creditcard') }}
    ), 

    customers as (
        select 
        customer_fk
        , customerid
        , personid
        , firstname
        , lastname 
        from {{ ref('dim_customers') }}
    ),   

    address1 as (
        select 
        address_fk
        , addressid	
        , addressline1			
        , city	
        , stateprovinceid
        , postalcode	
        from {{ ref('dim_address') }}
    ),

    reasons as (
        select 
        salesreason_fk
        , salesorderid
        , salesreasonid	
        , reason	
        , reasontype
        from {{ ref('dim_reasons') }}
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
        from {{ ref('stg_orders') }}
    ),
        
    orders_with_sk as(     
        select 
        salesorderid
        , customers.customer_fk
        , address1.address_fk
        , creditcard.creditcard_fk
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
        , address1.addressid 
        , shiptoaddressid 
        , shipmethodid 	
        , address1.addressline1			
        , address1.city	
        , address1.stateprovinceid
        , address1.postalcode	
        , creditcard.creditcardid
        , creditcard.cardtype
        from orders as orders_with_sk
        left join address1 on orders_with_sk.addressid = address1.addressid
        left join creditcard on orders_with_sk.creditcardid = creditcard.creditcardid
        right join customers on orders_with_sk.customerid = customers.customerid
        )

        select * from orders_with_sk
