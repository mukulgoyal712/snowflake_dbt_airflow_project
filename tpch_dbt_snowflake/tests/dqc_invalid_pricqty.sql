SELECT
    *
FROM
    {{ ref('lineitems_bz') }}
WHERE
    l_extendedprice <= 0 OR l_quantity <= 0