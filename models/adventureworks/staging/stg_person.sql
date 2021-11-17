with
        person as (
            select
            businessentityid
            , CONCAT(firstname,' ', lastname) as fullname
            from {{ source('erp_adventureworks','person')}} as person
        )
        select * from person