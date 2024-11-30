SELECT
    *
FROM {{ref('orders_bz')}} o
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ref('customers_bz')}} c
    WHERE o.o_custkey = c.c_custkey
)