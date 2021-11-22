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
        salesorderid
        , salesreasonid
        from {{ ref('stg_reasons_bridge') }}
    ),

    transformed as (
        select 
        reasons_bridge.salesorderid
        , reason
        , reasontype 
        , reasons_bridge.salesreasonid
        from  reasons as transformed
        left join reasons_bridge on transformed.salesreasonid = reasons_bridge.salesreasonid
        where reasons_bridge.salesreasonid is not null 
    ),

    grouped as (
        select
        salesorderid
        , STRING_AGG(reason, ', ') as salesreasons
        from transformed as grouped
        group by salesorderid
    )   

    select * from grouped