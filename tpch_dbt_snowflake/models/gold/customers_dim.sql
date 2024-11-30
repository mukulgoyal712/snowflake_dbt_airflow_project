{{
    config(
        pre_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('customers_dim', CURRENT_TIMESTAMP, 'started')"
        ],
        post_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('customers_dim', CURRENT_TIMESTAMP, 'completed')"
        ]
    )
}}


SELECT DISTINCT
    c_custkey,
    c_name,
    n.n_name as nation_name,
    r.region_name,
    c_acctbal,
    c_mktsegment

FROM
    {{ref('customers_sil')}} AS c
JOIN {{ref("nations_sil")}} AS n
    ON c.c_nationkey = n.n_nationkey
JOIN {{ref('raw_region_seed')}} AS r
    ON n.n_regionkey = r.region_id