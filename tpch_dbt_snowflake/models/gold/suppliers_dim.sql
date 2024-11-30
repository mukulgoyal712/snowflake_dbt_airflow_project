{{
    config(
        pre_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('suppliers_dim', CURRENT_TIMESTAMP, 'started')"
        ],
        post_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('suppliers_dim', CURRENT_TIMESTAMP, 'completed')"
        ]
    )
}}

SELECT
    s.s_suppkey as supplier_key,
    s.s_name as supplier_name,
    n.n_name as nation_name,
    r.region_name,
    s.s_acctbal as acctbal
FROM
    {{ref('suppliers_sil')}} AS s
JOIN {{ref("nations_sil")}} AS n
    ON s.s_nationkey = n.n_nationkey
JOIN {{ref('raw_region_seed')}} AS r
    ON n.n_regionkey = r.region_id