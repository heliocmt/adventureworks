{{ config(materialized='table') }}
with
        source_data as (
            select
            row_number() over (order by productid) as product_fk
            , productid	
            , name as product_name
            , productnumber	
            from {{ source('erp_adventureworks','prooducts')}}
        )
        select * from source_data