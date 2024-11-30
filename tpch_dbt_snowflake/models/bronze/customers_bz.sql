{{
    config
    (
        description= "Customer Detail Table",
        enabled= True,
        pre_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('customers_bz', CURRENT_TIMESTAMP, 'started')"
        ],
        post_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('customers_bz', CURRENT_TIMESTAMP, 'completed')"
        ]
    )
}}


select DISTINCT
    c_custkey,
    TRIM(c_name) AS c_name,
    c_address,
    c_phone,
    c_nationkey,
    c_acctbal,
    TRIM(c_mktsegment) AS c_mktsegment,
    c_comment,
    _etl_loaded_at
from 
    {{source('staging','stg_customer')}}