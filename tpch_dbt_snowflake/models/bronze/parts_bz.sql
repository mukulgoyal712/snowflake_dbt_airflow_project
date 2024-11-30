{{
    config(
        pre_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('parts_bz', CURRENT_TIMESTAMP, 'started')"
        ],
        post_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('parts_bz', CURRENT_TIMESTAMP, 'completed')"
        ]
    )
}}

SELECT
    p_partkey,
    TRIM(p_name) AS p_name,
    TRIM(p_mfgr) AS p_mfgr,
    TRIM(p_brand) AS p_brand,
    TRIM(p_type) AS p_type,
    p_size::NUMBER AS p_size,
    p_container,
    p_retailprice::DECIMAL AS p_retailprice,
    p_comment
    ,_etl_loaded_at
FROM
    {{source('staging','stg_part')}}