with
        source_data as (
            select
            row_number() over (order by creditcardid) as creditcard_fk
            , creditcardid
            , cardtype
            from {{ source('erp_adventureworks','creditcard')}}
        )
        select * from source_data