{{ config(materialized='table') }}

with
    staging as (
        select * from {{ ref('stg_creditcard') }}
    ),
    transformed as (
        select       
        creditcard_fk
        , creditcardid
        , cardtype
        from staging
    )
    select * from transformed