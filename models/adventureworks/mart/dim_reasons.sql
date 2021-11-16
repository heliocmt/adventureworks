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
        , salesorderid
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
        , billtoaddressid 
        , shiptoaddressid 
        , shipmethodid 
        , creditcardid 
        from  {{ ref('stg_orders') }}
    ),
    reasons as (
        select
        transformed.salesorderid
        , salesorder_fk
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
        , billtoaddressid 
        , shiptoaddressid 
        , shipmethodid 
        , creditcardid 
        , transformed.salesorder_sk	
        , transformed.salesreasonid
        , transformed.reason
        , transformed.reasontype
        from orders as reasons
        right join transformed on reasons.salesorder_fk = transformed.salesorder_sk
    )
    select * from reasons
    order by salesorderid