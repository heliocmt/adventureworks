{{ config(materialized='table') }}

with
    staging as (
        select * from {{ ref('stg_reasons') }}
    ),
    transformed as (
        select
        row_number() over (order by salesorderid) as salesorder_fk
        salesorderid		
        , salesreasonid
        , source_data1.reason
        , source_data1.reasontype
        from staging
    )
    select * from transformed