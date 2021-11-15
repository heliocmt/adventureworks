{{ config(materialized='table') }}

with
    staging as (
        select * from {{ ref('stg_reasons') }}
    ),
    transformed as (
        select
        row_number() over (order by salesreasonid) as salesreason_fk
        , salesorderid		
        , salesreasonid
        , reason
        , reasontype
        from staging
    )
    select * from transformed