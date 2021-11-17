{{ config(materialized='table') }}

with
    stg_address as (
        select         
        row_number() over (order by addressid) as address_fk
        , addressid	
        , addressline1
        , city	
        , stateprovinceid
        , postalcode 
        from {{ ref('stg_address') }}
    ),
    stg_province as (
        select             
        province
        , countryregioncode
        , territoryid		
        , stateprovincecode
        , stateprovinceid 
        from {{ ref('stg_province') }}
    ),
    stg_country as (
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
        , stg_province.countryregioncode
        , stg_province.province
        , stg_province.territoryid		
        , stg_province.stateprovincecode
        , stg_province.stateprovinceid
        from stg_address as source_data
        left join stg_province on source_data.stateprovinceid = stg_province.stateprovinceid
    ),

    source_data2 as (
        select
        addressid
        , address_fk	
        , addressline1			
        , city	
        , stg_country.country
        , province
        , territoryid		
        , stateprovincecode
        , stg_country.countryregioncode 
        , stateprovinceid
        from source_data as source_data2
        left join stg_country on source_data2.countryregioncode = stg_country.countryregioncode
    )
    select * from source_data2