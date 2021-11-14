with
        source_data1 as (
            select
            salesreasonid		
            , name as reason		
            , reasontype
            from {{ source('erp_adventureworks','reasons')}}
        )
        source_data as (
            select
            salesorderid		
            , salesreasonid
            , source_data1.reason
            , source_data1.reasontype
            from {{ source('erp_adventureworks','reasonsbridge')}}
            left join source_data1 on source_data.salesorderid = source_data1.salesorderid 
        )
        select * from source_data