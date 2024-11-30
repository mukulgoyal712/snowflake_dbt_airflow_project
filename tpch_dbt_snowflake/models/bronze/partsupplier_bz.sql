{{
    config(
        pre_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('partsupplier_bz', CURRENT_TIMESTAMP, 'started')"
        ],
        post_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('partsupplier_bz', CURRENT_TIMESTAMP, 'completed')"
        ]
    )
}}


SELECT
    {{dbt_utils.generate_surrogate_key([
        'ps_partkey',
        'ps_suppkey'])
    }} AS ps_part_supp_key,
    ps_partkey,
    ps_suppkey,
    ps_availqty::NUMBER AS ps_availqty,
    ps_supplycost::DECIMAL AS ps_supplycost,
    ps_comment,
    null_column,
    _etl_loaded_at
FROM 
    {{source('staging','stg_partsupp')}}