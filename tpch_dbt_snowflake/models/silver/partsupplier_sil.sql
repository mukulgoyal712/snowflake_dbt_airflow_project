
{{
    config(
        materialized='incremental',
        on_schema_change='append_new_columns',
        unique_key='ps_part_supp_key',
        clustered_by = [
            'ps_part_supp_key'
        ],
        pre_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('partsupplier_sil', CURRENT_TIMESTAMP, 'started')"
        ],
        post_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('partsupplier_sil', CURRENT_TIMESTAMP, 'completed')"
        ]
    )
}}


SELECT
    ps_part_supp_key,
    ps_partkey,
    ps_suppkey,
    ps_availqty,
    ps_supplycost,
    _etl_loaded_at
FROM 
    {{ref('partsupplier_bz')}}

{% if is_incremental() %}
    WHERE _etl_loaded_at > (SELECT COALESCE(MAX(_etl_loaded_at),'1900-01-01') FROM {{this}})
{% endif %}