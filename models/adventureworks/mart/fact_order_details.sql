{{ config(materialized='table') }}

with


    products as (
        select
        product_fk
        , productid	
        , product_name	
        , productnumber	
        from {{ ref('dim_products') }}
    ),