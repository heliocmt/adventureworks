{{ config(materialized='table') }}

with
        customers as (
            select 
            customerid	
            , personid	
            from {{ source('erp_adventureworks','customers')}} as customers
        ),
            
        person_bridge as (
            select
            businessentityid
            , personid
            from {{ source('erp_adventureworks','personbridge')}} as person_bridge
        ),

        person as (
            select
            row_number() over (order by firstname) as customer_sk
            , businessentityid
            , persontype	
            , namestyle	
            , title
            , firstname	
            , middlename	
            , lastname
            from {{ source('erp_adventureworks','person')}} as person
        ),

        person2 as (
            select 
            row_number() over (order by person_bridge.personid) as customer_fk
            , person_bridge.businessentityid
            , person_bridge.personid
            , customerid
            from customers as person2
            right join person_bridge on person2.personid = person_bridge.personid
        ),

        source_data as (
            select
            customer_fk
            , person2.businessentityid
            , person2.customerid
            , person2.personid
            , firstname
            , lastname
            from person as source_data
            left join person2 on source_data.customer_sk = person2.customer_fk
        )
        select * from source_data
    