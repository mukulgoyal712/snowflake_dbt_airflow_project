{% snapshot customers_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='c_custkey', 
        strategy='timestamp',
        updated_at='_etl_loaded_at'
    )
}}


    SELECT
    c_custkey,
    c_name,
    c_address,
    c_phone,
    c_nationkey,
    c_acctbal,
    c_mktsegment,
    -- c_comment,
    _etl_loaded_at
FROM {{ ref('customers_bz') }} 
    
{% endsnapshot %}
