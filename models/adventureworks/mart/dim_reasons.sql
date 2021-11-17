{{ config(materialized='table') }}

with
    reasons as (
        select 	
        salesreasonid
        , reason
        , reasontype 
        from {{ ref('stg_reasons') }}
    ),

    reasons_bridge as (
        select 
        row_number() over (order by salesorderid) as salesorder_sk
        , salesorderid
        , salesreasonid	
        from {{ ref('stg_reasons_bridge') }}
    ),

    transformed as (
        select 
        reasons_bridge.salesorderid
        , reasons_bridge.salesorder_sk
        , reason
        , reasontype 
        , reasons_bridge.salesreasonid
        from  reasons as transformed
        left join reasons_bridge on transformed.salesreasonid = reasons_bridge.salesreasonid
    )   
    select * from transformed