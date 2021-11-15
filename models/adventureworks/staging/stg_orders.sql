with
        source_data as (
            select
            salesorderid
            , customerid 
            , orderdate 
            , duedate 
            , shipdate 
            , subtotal 
            , taxamt 
            , freight 
            , totaldue
            , territoryid 
            , status
            , billtoaddressid 
            , shiptoaddressid 
            , shipmethodid 
            , creditcardid 
            from {{ source('erp_adventureworks','orders')}}
        )
        select * from source_data