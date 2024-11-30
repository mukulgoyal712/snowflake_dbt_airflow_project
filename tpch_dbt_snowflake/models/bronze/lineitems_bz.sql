{{
    config(
        pre_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('lineitems_bz', CURRENT_TIMESTAMP, 'started')"
        ],
        post_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('lineitems_bz', CURRENT_TIMESTAMP, 'completed')"
        ]
    )
}}

--on_schema_change = ignore,fail,append_new_columns,sync_all_columns
--unique_key will decide inserts & updates
--no unique_key then only inserts
SELECT
    {{
        dbt_utils.generate_surrogate_key([
            'l_orderkey',
            'l_linenumber'])
    }} as l_order_item_key,
    l_orderkey,
    l_partkey,
    l_suppkey,
    l_linenumber,
    IFF(l_quantity IS NULL,0,l_quantity)::DECIMAL(10,2) AS l_quantity,
    IFF(l_extendedprice IS NULL,0,l_extendedprice)::DECIMAL(10,2) AS l_extendedprice,
    IFF(l_discount IS NULL,0,l_discount)::DECIMAL(10,2) AS l_discount,
    IFF(l_tax IS NULL,0,l_tax)::DECIMAL(10,2) AS l_tax,
    IFF(l_returnflag IS NULL,'U',l_returnflag) AS l_returnflag,
    COALESCE(l_linestatus,'U') AS l_linestatus,
    TO_DATE(l_shipdate) as l_shipdate,
    TO_DATE(l_commitdate) as l_commitdate,
    TO_DATE(l_receiptdate) as l_receiptdate,
    TRIM(l_shipinstruct) AS l_shipinstruct,
    COALESCE(l_shipmode,'Unknown') AS l_shipmode
    ,_etl_loaded_at
FROM
    {{source('staging','stg_lineitem')}}

