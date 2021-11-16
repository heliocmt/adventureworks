{{ config(materialized='table') }}

with
    staging as (
        select             
        customer_fk
        , customerid
        , personid
        , firstname
        , lastname 
        from {{ ref('stg_customers') }} as staging
    )
    select * from staging
