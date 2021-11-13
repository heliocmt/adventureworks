with
        source_data as (
            select
            creditcardid
            , cardtype
            from {{ source('erp_adventureworks','creditcard')}}
        )
        select * from source_data