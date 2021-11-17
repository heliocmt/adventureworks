{{ config(materialized='table') }}

with
    address1 as (
        select         
        row_number() over (order by addressid) as address_fk
        , addressid	
        , addressline1
        , city	
        , stateprovinceid
        , postalcode 
        from {{ ref('stg_address') }}
    ),
    province1 as (
        select             
        province
        , countryregioncode
        , territoryid		
        , stateprovincecode
        , stateprovinceid 
        from {{ ref('stg_province') }}
    ),
    country1 as (
        select 
        countryregioncode
        , country 
        from {{ ref('stg_country') }}
    ),
    
    source_data as (
        select
        addressid
        , address_fk	
        , addressline1			
        , city	
        , province1.countryregioncode
        , province1.province
        , province1.territoryid		
        , province1.stateprovincecode
        , province1.stateprovinceid
        from address1 as source_data
        left join province1 on source_data.stateprovinceid = province1.stateprovinceid
    ),

    source_data2 as (
        select
        addressid
        , address_fk	
        , addressline1			
        , city	
        , country1.country
        , province
        , territoryid		
        , stateprovincecode
        , country1.countryregioncode 
        , stateprovinceid
        from source_data as source_data2
        left join country1 on source_data2.countryregioncode = country1.countryregioncode
    )
    select * from source_data2