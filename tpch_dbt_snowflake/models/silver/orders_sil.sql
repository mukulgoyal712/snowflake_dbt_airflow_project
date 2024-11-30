{{
    config(
        materialized = 'incremental',
        on_schema_change = 'append_new_columns',
        unique_key = 'o_order_cust_key',
        pre_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('orders_sil', CURRENT_TIMESTAMP, 'started')"
        ],
        post_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('orders_sil', CURRENT_TIMESTAMP, 'completed')"
        ],
        clustered_by =[
            'o_order_cust_key'
        ]

    )
}}

SELECT
    o_order_cust_key,
    o_orderkey,
    o_custkey,
    COALESCE(o_orderstatus,'U') AS o_orderstatus,
    COALESCE(o_totalprice,0) AS o_totalprice,
    o_orderdate,
    COALESCE(o_orderpriority,'Unknown') AS o_orderpriority,
    o_clerk,
    COALESCE(o_shippriority,'U') AS o_shippriority,
    _etl_loaded_at
FROM
    {{ref('orders_bz')}}

{% if is_incremental() %}
    WHERE _etl_loaded_at > (SELECT COALESCE(MAX(_etl_loaded_at),'1900-01-01') FROM {{this}})
{% endif %}