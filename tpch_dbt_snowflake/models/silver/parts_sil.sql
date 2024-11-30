{{
    config(
        materialized='incremental',
        on_schema_change='append_new_columns',
        unique_key='p_partkey',
        clustered_by =[
            'p_partkey'
        ],
        pre_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('parts_sil', CURRENT_TIMESTAMP, 'started')"
        ],
        post_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('parts_sil', CURRENT_TIMESTAMP, 'completed')"
        ]
    )
}}

SELECT
    p_partkey,
    p_name,
    p_mfgr,
    p_brand,
    p_type,
    p_size,
    p_container,
    p_retailprice,
    _etl_loaded_at
FROM
    {{ref("parts_bz")}}

{% if is_incremental() %}
    WHERE _etl_loaded_at > (SELECT COALESCE(MAX(_etl_loaded_at),'1900-01-01') FROM {{this}})
{% endif %}