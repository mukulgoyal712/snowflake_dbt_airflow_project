{{
    config(
        pre_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('nations_bz', CURRENT_TIMESTAMP, 'started')"
        ],
        post_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('nations_bz', CURRENT_TIMESTAMP, 'completed')"
        ]
    )
}}

SELECT
    n_nationkey,
    TRIM(IFF(n_name IS NULL, 'Unknown',n_name)) AS n_name,
    n_regionkey,
    n_comment,
    _etl_loaded_at
FROM
    {{source('staging','stg_nation')}}