with
        source1 as (
            select
            salesreasonid		
            , name as reason		
            , reasontype
            from {{ source('erp_adventureworks','reasons')}}
        ),
        source_data as (
            select
            salesorderid		
            , source1.salesreasonid
            , source1.reason
            , source1.reasontype
            from {{ source('erp_adventureworks','reasonsbridge')}} as source_data
            left join source1 on source_data.salesreasonid = source1.salesreasonid 
        )
        select * from source_data