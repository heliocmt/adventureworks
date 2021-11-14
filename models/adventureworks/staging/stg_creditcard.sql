with
        source_data1 as (
            select
            creditcardid
            , cardtype
            from {{ source('erp_adventureworks','creditcard')}}
        )
        select * from source_data