{{ config(materialized='table') }}

with
    staging as (
        select * from {{ ref('stg_products') }}
    ),
    transformed as (
        select
        product_fk
        , productid	
        , product_name	
        , productnumber	
        from staging
    )
    select * from transformed
