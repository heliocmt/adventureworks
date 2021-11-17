with
        person_bridge as (
            select
            businessentityid
            , customerid
            from {{ source('erp_adventureworks','businessentitycontact')}}
        )
        select * from person_bridge