{% snapshot suppliers_snapshot %}

{{
    config(
        target_schema='snapshots',
        strategy= 'check',
        check_cols= [
            's_name',
            's_address',
            's_nationkey', 
            's_phone',
            's_acctbal'
        ],
        unique_key= 's_suppkey'
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
FROM
    {{ref("suppliers_bz")}}

{% endsnapshot %}