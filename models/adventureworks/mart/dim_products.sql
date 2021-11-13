{{ config(materialized='table') }}

with
    staging as (
        select * from {{ ref('stg_products') }}
    ),
    transformed as (
        select
        row_number() over (order by productid) as product_fk
        , productid	
        , "name" as product_name	
        , productnumber	
        from staging
    )
    select * from transformed
