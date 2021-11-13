{{ config(materialized='table') }}

with
    staging as (
        select * from {{ ref('stg_customers') }}
    ),
    transformed as (
        select
        row_number() over (order by customerid) as customer_fk
        , customerid	
        , personid	
        , territoryid
        from staging
    )
    select * from transformed
