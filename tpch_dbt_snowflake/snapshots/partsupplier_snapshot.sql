{% snapshot partsupplier_snapshot %}

{{
    config(
        target_schema = 'snapshots',
        unique_key = 'ps_part_supp_key',
        strategy = 'timestamp',
        updated_at = '_etl_loaded_at'
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

{% endsnapshot %}