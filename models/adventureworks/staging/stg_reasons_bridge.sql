    with 
        source_data as (
            select
            salesorderid		
            , salesreasonid
            from {{ source('erp_adventureworks','reasonsbridge')}} as source_data
        )
        select * from source_data