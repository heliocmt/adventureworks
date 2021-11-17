with
        person as (
            select
            businessentityid
            , persontype	
            , namestyle	
            , title
            , firstname	
            , middlename	
            , lastname
            from {{ source('erp_adventureworks','person')}} as person
        )
        select * from person