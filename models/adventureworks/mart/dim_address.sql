{{ config(materialized='table') }}

with
    staging as (
        select * from {{ ref('stg_address') }}
    ),
    transformed as (
        select        
        row_number() over (order by addressid) as address_fk
        , addressid	
        , addressline1			
        , city	
        , province
        , country
        , postalcode		
        , countryregioncode		
        , stateprovincecode
        , stateprovinceid
        from staging
    )
    select * from transformed