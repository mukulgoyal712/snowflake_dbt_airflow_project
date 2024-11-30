SELECT
    *
FROM {{ref('lineitems_bz')}}
WHERE l_tax <0 OR l_tax >=1