with
        source_data as (
            select
            row_number() over (order by salesorderid) as salesorderid_sk
            , row_number() over (order by customerid) as salescustomer_sk
            , salesorderid
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