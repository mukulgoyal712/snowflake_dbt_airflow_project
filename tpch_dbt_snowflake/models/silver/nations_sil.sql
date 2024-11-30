{{
    config(
        description = "Implementing Upsert method on nations",
        materialized = 'incremental',
        enabled = true,
        on_schema_change = 'append_new_columns',
        unique_key = 'n_nationkey',
        pre_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('nations_sil', CURRENT_TIMESTAMP, 'started')"
        ],
        post_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('nations_sil', CURRENT_TIMESTAMP, 'completed')"
        ],
        clustered_by = [
            "n_nationkey"
        ]
    )
}}


SELECT
    n_nationkey,
    n_name,
    n_regionkey,
    n_comment,
    _etl_loaded_at
FROM
    {{ref("nations_bz")}}

{% if is_incremental() %}
    WHERE _etl_loaded_at > (SELECT COALESCE(MAX(_etl_loaded_at),'1900-01-01') FROM {{this}})
{% endif %}