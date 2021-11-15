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
        , billtoaddressid as addressid 
        , shiptoaddressid 
        , shipmethodid 
        , creditcardid	
        from {{ ref('stg_orders') }}
    ),
    
    orders2 as (
        select
        orders.salesorderid
        , salesreason_fk
        , orders.customerid 
        , orders.orderdate 
        , orders.duedate 
        , orders.shipdate 
        , orders.subtotal 
        , orders.taxamt 
        , orders.freight 
        , orders.totaldue
        , orders.territoryid 
        , orders.addressid 
        , orders.shiptoaddressid 
        , orders.shipmethodid 
        , orders.creditcardid
        , salesreasonid	
        , reason	
        , reasontype
        from reasons as orders2
        left join orders on orders2.salesorderid = orders.salesorderid
    ),
        
    orders_with_sk as(     
        select
        salesorderid
        , customers.customer_fk
        , address1.address_fk
        , salesreason_fk
        , creditcard.creditcard_fk
        , customers.customerid	
        , customers.personid	
        , customers.territoryid
        , orderdate 
        , duedate 
        , shipdate 
        , subtotal 
        , taxamt 
        , freight 
        , totaldue
        , address1.addressid 
        , shiptoaddressid 
        , shipmethodid 	
        , address1.addressline1			
        , address1.city	
        , address1.stateprovinceid
        , address1.postalcode	
        , salesreasonid	
        , reason	
        , reasontype
        , creditcard.creditcardid
        , creditcard.cardtype
        from orders2 as orders_with_sk
        left join address1 on orders_with_sk.addressid = address1.addressid
        left join creditcard on orders_with_sk.creditcardid = creditcard.creditcardid
        left join customers on orders_with_sk.customerid = customers.customerid
        order by salesorderid asc
        )

        select * from orders_with_sk
