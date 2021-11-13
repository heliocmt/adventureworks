{{ config(materialized='table') }}
with
        source_data1 as (
            select
            countryregioncode
            , name as country
            from {{ source('erp_adventureworks','countryregion')}} as source_data1
            ),

        source_data2 as (
            select
            source_data1.countryregioncode 
            , source_data1.country
            , name as province
            , territoryid		
            , stateprovincecode
            , stateprovinceid
            from {{ source('erp_adventureworks','stateprovince')}} as source_data2
            left join source_data1 on source_data2.countryregioncode = source_data1.countryregioncode
        ),

        source_data as (
            select
            addressid	
            , addressline1			
            , city	
            , source_data2.country
            , postalcode		
            , source_data2.countryregioncode		
            , source_data2.province
            , source_data2.territoryid		
            , source_data2.stateprovincecode
            , source_data2.stateprovinceid
            from {{ source('erp_adventureworks','address')}} as source_data
            left join source_data2 on source_data.stateprovinceid = source_data2.stateprovinceid
            order by addressid asc
        )
        select * from source_data