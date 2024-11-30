{{
    config(
        description = "implementing upsert method for customers data",
        materialized = 'incremental',
        unique_key = 'c_custkey',
        pre_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('customers_sil', CURRENT_TIMESTAMP, 'started')"
        ],
        post_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('customers_sil', CURRENT_TIMESTAMP, 'completed')"
        ],
        clustered_by = [
            "c_custkey"
        ]

    )
}}


SELECT
    c_custkey,
    c_name,
    c_address,
    c_phone,
    c_nationkey,
    c_acctbal,
    c_mktsegment,
    _etl_loaded_at
FROM {{ref("customers_bz")}}

{% if is_incremental() %}

    where _etl_loaded_at >=(select coalesce(max(_etl_loaded_at),'1900-01-01') from {{this}})

{% endif %}