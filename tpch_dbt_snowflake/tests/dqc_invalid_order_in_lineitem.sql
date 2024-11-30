SELECT
    *
FROM {{ref('lineitems_bz')}} l
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ref('orders_bz')}} o
    WHERE l.l_orderkey = o.o_orderkey
)