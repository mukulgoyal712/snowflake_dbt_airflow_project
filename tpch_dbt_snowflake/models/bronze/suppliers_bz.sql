{{
    config(
        pre_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('suppliers_bz', CURRENT_TIMESTAMP, 'started')"
        ],
        post_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('suppliers_bz', CURRENT_TIMESTAMP, 'completed')"
        ]
    )
}}


SELECT
    s_suppkey,
    TRIM(s_name) AS s_name,
    TRIM(s_address) AS s_address,
    s_nationkey,
    TRIM(s_phone) AS s_phone,
    s_acctbal::DECIMAL AS s_acctbal,
    s_comment,
    _etl_loaded_at
FROM
    {{source('staging','stg_suppliers')}}