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
        , territoryid
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
        , salesorder_fk
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
        , status
        , territoryid 
        , billtoaddressid as addressid 
        , shiptoaddressid 
        , shipmethodid 
        , creditcardid	
        from {{ ref('stg_orders') }}
    )
    
    orders_with_sk as (
        select
        , address1.address_fk
        , reasons.salesreason_fk
        , customers.customer_fk
        , creditcard.creditcard_fk
        , customers.customerid	
        , customers.personid	
        , customers.territoryid
        , salesorderid
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
        , reasons.salesreasonid	
        , reasons.reason	
        , reasons.reasontype
        , creditcard.creditcardid
        , creditcard.cardtype
        from orders
        left join address1 on orders_with_sk.addressid = address1.addressid
        left join reasons on orders_with_sk.salesorderid = reasons.salesorderid
        left join creditcard on orders_with_sk.creditcardid = creditcard.creditcardid
        left join customers on orders_with_sk.customerid = customers.customerid
        )
        select * from orders_with_sk
