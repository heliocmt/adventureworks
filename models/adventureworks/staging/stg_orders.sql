with
        source_data as (
            select
            salesorderid 
            , orderdate 
            , duedate 
            , shipdate 
            , "status"
            , customerid
            , territoryid 
            , billtoaddressid 
            , shiptoaddressid 
            , shipmethodid 
            , creditcardid 
            , subtotal 
            , taxamt 
            , freight 
            , totaldue
            from {{ source('erp_adventureworks','orders')}}
        )
        select * from source_data