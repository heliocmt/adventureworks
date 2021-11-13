{{ config(materialized='table') }}

with
    creditcard as (
        select 
        creditcard_fk
        , creditcardid
        , cardtype
        from {{ ref('dim_creditcard') }}
    ), 

        customers as (
        select 
        customer_fk
        , customerid	
        , personid	
        , territoryid
        from {{ ref('dim_customers') }}
    ),   

    address1 as (
        select 
        address_fk
        , addressid	
        , addressline1			
        , city	
        , stateprovinceid
        , postalcode	
        from {{ ref('dim_address') }}
    ),

    reasons as (
        select 
        salesreason_fk
        salesreasonid	
        , reason	
        , reasontype
        from {{ ref('dim_reasons') }}
    ), 