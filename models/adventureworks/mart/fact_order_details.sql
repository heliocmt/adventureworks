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
    order_details as (
        select	
        salesorderid
        , productid		
        , unitprice		
        , orderqty		
        , salesorderdetailid	
        , unitpricediscount		
        from {{ ref('stg_order_details') }}
    ),
    order_details_with_sk as (
        select	
        salesorderid	
        , products.product_fk
        , products.productid
        , products.product_name	
        , unitprice		
        , orderqty		
        , products.productnumber	
        , salesorderdetailid	
        , unitpricediscount		
        from order_details as order_details_with_sk
        left join products on order_details_with_sk.productid = products.productid
    )
    select * from order_details_with_sk