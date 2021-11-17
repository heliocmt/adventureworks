{{ config(materialized='table') }}

with
    customer as (
        select           
        customer_fk  
        , customerid
        , personid
        , storeid
        from {{ ref('stg_customers') }} as customer
    ),

    person as (
        select
        businessentityid
        , persontype	
        , fullname
        from {{ ref('stg_person')}} as person
    ),

    store as (
        select
        storecustomerid
        , storename	
        from {{ ref('stg_store')}}
    ),

    final as (
        select
        customer_fk
        , customerid
        , personid
        , storeid
        , person.businessentityid
        , person.fullname
        , store.storecustomerid
        , store.storename
        , case
        when person.fullname is not null
        then person.fullname
        else store.storename
        end as customername
        from customer as final
        left join person on final.personid = person.businessentityid
        left join store on final.storeid = store.storecustomerid
    )
    select * from final