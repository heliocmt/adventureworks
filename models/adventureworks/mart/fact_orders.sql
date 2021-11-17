{{ config(materialized='table') }}
with

    creditcard as (
        select 
        creditcard_fk
        , creditcardid
        , cardtype
        from {{ ref('dim_creditcard') }} as creditcard
    ), 

    customers as (
        select 
        customer_fk
        , customerid
        , personid
        , storeid
        , businessentityid
        , fullname
        , storecustomerid
        , storename
        , case
        when person.fullname is not null
        then person.fullname
        else store.storename
        end as customername
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
        , creditcard.creditcard_fk
        , orderdate 
        , duedate 
        , shipdate 
        , subtotal 
        , taxamt 
        , freight 
        , totaldue
        , status
        , shiptoaddressid 
        , shipmethodid 	
        from orders as orders_with_sk
        left join customers on orders_with_sk.customerid = customers.customerid
        left join address1 on orders_with_sk.addressid = address1.addressid
        left join creditcard on orders_with_sk.creditcardid = creditcard.creditcardid
        )
        select * from orders_with_sk
        