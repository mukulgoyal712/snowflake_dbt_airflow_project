{{
    config(
        pre_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('orders_bz', CURRENT_TIMESTAMP, 'started')"
        ],
        post_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('orders_bz', CURRENT_TIMESTAMP, 'completed')"
        ]
    )
}}


SELECT 
    {{
        dbt_utils.generate_surrogate_key([
        'o_orderkey',
        'o_custkey'])
    }} as o_order_cust_key,
    o_orderkey,
    o_custkey,
    o_orderstatus,
    o_totalprice::DECIMAL(10,2) AS o_totalprice,
    o_orderdate::DATE AS o_orderdate,
    o_orderpriority,
    o_clerk,
    o_shippriority,
    o_comment,
    _etl_loaded_at
FROM 
    {{source('staging','stg_orders')}}