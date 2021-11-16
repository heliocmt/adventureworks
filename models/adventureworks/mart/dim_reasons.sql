{{ config(materialized='table') }}

with
    staging as (
        select * from {{ ref('stg_reasons') }}
    ),
    transformed as (
        select
        row_number() over (order by salesorderid) as salesorder_sk
        , salesorderid		
        , salesreasonid
        , reason
        , reasontype
        from staging
    ),
    orders as (
        select 
        row_number() over (order by salesorderid) as salesorder_fk
        , customer_fk
        , address_fk
        , creditcard_fk
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
        , stateprovinceid
        , postalcode	
        , creditcardid
        , cardtype
        from  {{ ref('fact_orders') }}
    ),
    reasons as (
        select
        transformed.salesorderid
        , salesorder_fk
        , customer_fk
        , address_fk
        , creditcard_fk
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
        , stateprovinceid
        , postalcode	
        , creditcardid
        , cardtype
        , transformed.salesorder_sk	
        , transformed.salesreasonid
        , transformed.reason
        , transformed.reasontype
        from orders as reasons
        left join transformed on reasons.salesorder_fk = transformed.salesorder_sk
    )
    select * from reasons
    order by salesorderid