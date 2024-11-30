SELECT 
    *
FROM {{ref('orders_bz')}}
WHERE o_orderstatus NOT IN ('O','F','P')