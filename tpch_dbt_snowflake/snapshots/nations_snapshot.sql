{% snapshot nations_snapshot %}

{{
    config(
        target_schema= 'snapshots',
        unique_key='n_nationkey', 
        strategy='timestamp',
        updated_at='_etl_loaded_at'
    )
}}

    SELECT
    n_nationkey,
    n_name,
    n_regionkey,
    -- n_comment,
    _etl_loaded_at
FROM {{ ref('nations_bz') }}
    
{% endsnapshot %}
