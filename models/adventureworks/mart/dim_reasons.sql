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
    order_details as (
        select
        row_number() over (order by salesorderid) as salesorder_fk
        , salesorderid			
        , orderqty		
        , unitprice		
        , unitpricediscount	
        from  {{ ref('stg_order_details') }}
    ),
    reasons as (
        select
        transformed.salesorderid 
        , transformed.salesorder_sk	
        , transformed.salesreasonid
        , transformed.reason
        , transformed.reasontype
        , salesorder_fk
        , orderqty		
        , unitprice		
        , unitpricediscount	
        from order_details as reasons
        right join transformed on reasons.salesorder_fk = transformed.salesorder_sk
    )
    select * from reasons