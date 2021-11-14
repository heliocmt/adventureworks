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
            , status
            , territoryid 
            , billtoaddressid 
            , shiptoaddressid 
            , shipmethodid 
            , creditcardid 
            from {{ source('erp_adventureworks','fact_orders')}}
        )
        select * from source_data