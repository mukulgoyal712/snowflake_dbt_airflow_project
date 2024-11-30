{{
    config(
        materialized = 'incremental',
        unique_key = 's_suppkey',
        on_schema_change= 'append_new_columns',
        clustered_by = [
            's_suppkey'
        ],
        pre_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('suppliers_sil', CURRENT_TIMESTAMP, 'started')"
        ],
        post_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('suppliers_sil', CURRENT_TIMESTAMP, 'completed')"
        ]
    )
}}


SELECT
    s_suppkey,
    s_name,
    s_address,
    s_nationkey,
    s_phone,
    s_acctbal,
    _etl_loaded_at

FROM {{ref('suppliers_bz')}}

{% if is_incremental() %}
    WHERE _etl_loaded_at > (SELECT COALESCE(MAX(_etl_loaded_at),'1900-01-01')  FROM {{this}})
{% endif %}