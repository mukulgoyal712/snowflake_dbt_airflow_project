{% snapshot parts_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key = 'p_partkey',
        strategy = 'timestamp',
        updated_at = '_etl_loaded_at'
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
    {{ref('parts_bz')}}

{% endsnapshot %}