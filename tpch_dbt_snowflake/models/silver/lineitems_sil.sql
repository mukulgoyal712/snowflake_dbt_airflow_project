{{
    config(
        materialized='incremental',
        on_schema_change = 'append_new_columns',
        unique_key ='l_order_item_key',
        pre_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('lineitems_sil', CURRENT_TIMESTAMP, 'started')"
        ],
        post_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('lineitems_sil', CURRENT_TIMESTAMP, 'completed')"
        ],
        clustered_by =[
            'l_order_item_key'
        ]
    )
}}
 
--on_schema_change = ignore,fail,append_new_columns,sync_all_columns
--unique_key will decide inserts & updates
--no unique_key then only inserts
SELECT
    l_order_item_key,
    l_orderkey,
    l_partkey,
    l_suppkey,
    l_linenumber,
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax,
    l_returnflag,
    CASE WHEN l_returnflag = 'R' then 'Returned'
        WHEN l_returnflag = 'A' then 'Active'
        WHEN l_returnflag = 'N' then 'N/A'
    END AS l_return_status,
    l_linestatus,
    CASE WHEN l_linestatus ='F' then 'Fully delivered'
        WHEN l_linestatus = 'P' then 'Partially delivered'
        WHEN l_linestatus = 'O' then 'Not shipped'
    END as l_linestatus_desc,
    l_shipdate,
    l_commitdate,
    l_receiptdate,
    l_shipinstruct,
    l_shipmode
    ,_etl_loaded_at
FROM
    {{ref('lineitems_bz')}}
{% if is_incremental() %}
where _etl_loaded_at >=(select coalesce(max(_etl_loaded_at),'1900-01-01') from {{this}})
{% endif %}

