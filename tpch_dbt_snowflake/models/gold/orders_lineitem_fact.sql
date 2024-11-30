{{
    config(
        pre_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('orders_lineitem_fact', CURRENT_TIMESTAMP, 'started')"
        ],
        post_hook = [
            "INSERT INTO tpch_elt.audits.audit_schema_tables (schema, run_time, run_status) 
            VALUES ('orders_lineitem_fact', CURRENT_TIMESTAMP, 'completed')"
        ]
    )
}}


SELECT
    o.o_orderkey,
    o.o_custkey as cust_id,
    o.o_orderstatus as order_status,
    CASE WHEN UPPER(o.o_orderstatus) ='O' THEN 'Ordered'
        WHEN UPPER(o.o_orderstatus) ='F' THEN 'Delivered'
        WHEN UPPER(o.o_orderstatus) ='P' THEN 'In-transit'
    END AS order_status_desc,
    o.o_totalprice as order_sale_price,
    o.o_orderdate as order_date,
    MONTH(o.o_orderdate) AS order_month,
    YEAR(o.o_orderdate) AS order_year,
    CASE WHEN o.o_orderpriority LIKE '%1%' THEN 'Urgent'
        WHEN o.o_orderpriority LIKE '%2%' THEN 'High'
        WHEN o.o_orderpriority LIKE '%3%' THEN 'Medium'
        WHEN o.o_orderpriority LIKE '%4%' THEN 'Not Specified'
        WHEN o.o_orderpriority LIKE '%5%' THEN 'Low'
    END AS order_priority,
    o.o_clerk,
    o.o_shippriority as ship_priority,
    l.l_orderkey,
    l.l_partkey,
    l.l_suppkey as supplier_id,
    l.l_quantity as item_qty,
    l.l_extendedprice,
    l.l_discount,
    l.l_tax,
    {{ 
        sale_price('l.l_extendedprice','l.l_discount','l.l_tax') 
    }} as sale_price,
    l.l_returnflag as return_flag,
    l.l_return_status as return_status,
    l.l_linestatus as line_status,
    l.l_linestatus_desc as line_status_desc,
    l.l_shipdate as ship_date,
    l.l_commitdate as commit_date,
    l.l_receiptdate as receipt_date,
    {{
        days_between('l.l_commitdate','l.l_shipdate')
    }} as delay_in_ship,
    {{
        days_between('l.l_shipdate','l.l_receiptdate')
    }} as days_in_receipt,
    l.l_shipinstruct,
    l.l_shipmode,
    p.p_name as part_name,
    p.p_mfgr as manufacturer_name,
    p.p_brand as brand_name,
    p.p_type as part_type,
    p.p_container as part_container,
    p.p_retailprice
FROM 
    {{ref('orders_sil')}} as o
JOIN {{ref('lineitems_sil')}} as l
    ON o.o_orderkey = l.l_orderkey
JOIN {{ref('parts_sil')}} as p
    ON l.l_partkey = p.p_partkey