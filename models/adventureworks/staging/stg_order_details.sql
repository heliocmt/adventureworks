with
    order_details as (
        select
        salesorderid		
        , salesorderdetailid	
        , orderqty		
        , productid		
        , unitprice		
        , unitpricediscount	
        from {{ source('erp_adventureworks','order_details')}}
        ),
    
    products as (
        select
        product_fk
        , productid	
        , product_name
        , productnumber	
        from {{ ref('stg_products') }}
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