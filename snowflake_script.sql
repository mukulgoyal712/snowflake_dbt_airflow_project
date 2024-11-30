CREATE DATABASE IF NOT EXISTS TPCH_ELT;

create schema if not exists staging;

------------------------------------
-- 1. Customer Table
CREATE OR REPLACE TABLE stg_customer (
    c_custkey         INT NOT NULL,
    c_name            STRING,
    c_address         STRING,
    c_nationkey       INT,
    c_phone           STRING,
    c_acctbal         NUMERIC(15, 2),
    c_mktsegment      STRING,
    c_comment         STRING,
    null_column       STRING
    ,_etl_loaded_at TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);
-- 2. Lineitem Table

CREATE or replace TABLE stg_lineitem (
    l_orderkey        INT NOT NULL,
    l_partkey         INT NOT NULL,
    l_suppkey         INT NOT NULL,
    l_linenumber      INT NOT NULL,
    l_quantity        DECIMAL(15, 2),
    l_extendedprice   DECIMAL(15, 2),
    l_discount        DECIMAL(15, 2),
    l_tax             DECIMAL(15, 2),
    l_returnflag      CHAR(1),
    l_linestatus      CHAR(1),
    l_shipdate        DATE,
    l_commitdate      DATE,
    l_receiptdate     DATE,
    l_shipinstruct    VARCHAR(25),
    l_shipmode        VARCHAR(10),
    l_comment         VARCHAR(44),
    null_column       STRING
    ,_etl_loaded_at TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE or replace TABLE stg_nation (
    n_nationkey       INT NOT NULL,
    n_name            VARCHAR(25),
    n_regionkey       INT,
    n_comment         VARCHAR(152),
    null_column       STRING
    ,_etl_loaded_at TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- 4. Order Table
CREATE or replace TABLE stg_orders (
    o_orderkey        INT NOT NULL,
    o_custkey         INT,
    o_orderstatus     CHAR(1),
    o_totalprice      DECIMAL(15, 2),
    o_orderdate       DATE,
    o_orderpriority   VARCHAR(15),
    o_clerk           VARCHAR(15),
    o_shippriority    INT,
    o_comment         VARCHAR(79),
    null_column       STRING
    ,_etl_loaded_at TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- 5. Part Table
CREATE or replace TABLE stg_part (
    p_partkey         INT NOT NULL,
    p_name            VARCHAR(55),
    p_mfgr            VARCHAR(25),
    p_brand           VARCHAR(10),
    p_type            VARCHAR(25),
    p_size            INT,
    p_container       VARCHAR(10),
    p_retailprice     DECIMAL(15, 2),
    p_comment         VARCHAR(23),
    null_column       STRING
    ,_etl_loaded_at TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- 6. Partsupp Table
CREATE or replace TABLE stg_partsupp (
    ps_partkey        INT NOT NULL,
    ps_suppkey        INT NOT NULL,
    ps_availqty       INT,
    ps_supplycost     DECIMAL(15, 2),
    ps_comment        VARCHAR(199),
    null_column       STRING
    ,_etl_loaded_at TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- 7. Supplier Table

CREATE or replace TABLE stg_suppliers (
    s_suppkey         INT NOT NULL,
    s_name            VARCHAR(25),
    s_address         VARCHAR(40),
    s_nationkey       INT,
    s_phone           VARCHAR(15),
    s_acctbal         DECIMAL(15, 2),
    s_comment         VARCHAR(101),
    null_column       STRING
    ,_etl_loaded_at TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

---*********Data Loading*******
---*******************************
--I want to cdc and process only new files

--general configuration for connection and file format
create or replace storage integration s3_tpch_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::418295713957:role/s3-snowflake-tpch-elt-role' -- Put your arn data from AWS.
  STORAGE_ALLOWED_LOCATIONS = ('s3://s3-snowflake-tpch-elt/') --Put your S3 Location
  COMMENT = 'Please note that access was only given to the CSV files' ;
-- Using the DESC command to show the options in the new storage integration
DESC integration s3_tpch_integration;

CREATE OR REPLACE file format tpch_elt.staging.csv_fileformat
	type = 'csv'
	field_delimiter = '|'
	-- skip_header = 1
	null_if = ('NULL', 'null')
	empty_field_as_null = TRUE
	FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    -- ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    ;

--1. stage for customers

CREATE OR REPLACE stage tpch_elt.staging.customers_folder
	URL = 's3://s3-snowflake-tpch-elt/customers/'
	STORAGE_INTEGRATION = s3_tpch_integration
	FILE_FORMAT = tpch_elt.staging.csv_fileformat
    -- DIRECTORY = (ENABLE = TRUE)
    ;
list @tpch_elt.staging.customers_folder;

--2. stage for lineitems

CREATE OR REPLACE stage tpch_elt.staging.lineitems_folder
	URL = 's3://s3-snowflake-tpch-elt/lineitems/'
	STORAGE_INTEGRATION = s3_tpch_integration
	FILE_FORMAT = tpch_elt.staging.csv_fileformat;
list @tpch_elt.staging.lineitems_folder;

--3. stage for nation

CREATE OR REPLACE stage tpch_elt.staging.nation_folder
	URL = 's3://s3-snowflake-tpch-elt/nation/'
	STORAGE_INTEGRATION = s3_tpch_integration
	FILE_FORMAT = tpch_elt.staging.csv_fileformat;
list @tpch_elt.staging.nation_folder;

--4. stage for orders

CREATE OR REPLACE stage tpch_elt.staging.orders_folder
	URL = 's3://s3-snowflake-tpch-elt/orders/'
	STORAGE_INTEGRATION = s3_tpch_integration
	FILE_FORMAT = tpch_elt.staging.csv_fileformat;
list @tpch_elt.staging.orders_folder;

--5. stage for parts

CREATE OR REPLACE stage tpch_elt.staging.parts_folder
	URL = 's3://s3-snowflake-tpch-elt/parts/'
	STORAGE_INTEGRATION = s3_tpch_integration
	FILE_FORMAT = tpch_elt.staging.csv_fileformat;
list @tpch_elt.staging.parts_folder;

--5. stage for partsuppliers

CREATE OR REPLACE stage tpch_elt.staging.partsuppliers_folder
	URL = 's3://s3-snowflake-tpch-elt/partsuppliers/'
	STORAGE_INTEGRATION = s3_tpch_integration
	FILE_FORMAT = tpch_elt.staging.csv_fileformat;
list @tpch_elt.staging.partsuppliers_folder;

--6. stage for suppliers

CREATE OR REPLACE stage tpch_elt.staging.suppliers_folder
	URL = 's3://s3-snowflake-tpch-elt/suppliers/'
	STORAGE_INTEGRATION = s3_tpch_integration
	FILE_FORMAT = tpch_elt.staging.csv_fileformat;
list @tpch_elt.staging.suppliers_folder;

--loading files incrementally to staging
COPY INTO tpch_elt.staging.stg_customer 
    (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, null_column, _etl_loaded_at)
FROM (
    SELECT 
        $1 AS c_custkey, 
        $2 AS c_name, 
        $3 AS c_address, 
        $4 AS c_nationkey, 
        $5 AS c_phone, 
        $6 AS c_acctbal, 
        $7 AS c_mktsegment, 
        $8 AS c_comment, 
        $9 AS null_column, 
        CURRENT_TIMESTAMP() AS _etl_loaded_at
    FROM @tpch_elt.staging.customers_folder
);



COPY INTO tpch_elt.staging.stg_lineitem 
    (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment, null_column, _etl_loaded_at)
FROM (
    SELECT 
        $1 AS l_orderkey,
        $2 AS l_partkey,
        $3 AS l_suppkey,
        $4 AS l_linenumber,
        $5 AS l_quantity,
        $6 AS l_extendedprice,
        $7 AS l_discount,
        $8 AS l_tax,
        $9 AS l_returnflag,
        $10 AS l_linestatus,
        $11 AS l_shipdate,
        $12 AS l_commitdate,
        $13 AS l_receiptdate,
        $14 AS l_shipinstruct,
        $15 AS l_shipmode,
        $16 AS l_comment,
        $17 AS null_column,
        CURRENT_TIMESTAMP() AS _etl_loaded_at
    FROM @tpch_elt.staging.lineitems_folder
);


COPY INTO tpch_elt.staging.stg_nation 
    (n_nationkey, n_name, n_regionkey, n_comment, null_column, _etl_loaded_at)
FROM (
    SELECT 
        $1 AS n_nationkey,
        $2 AS n_name,
        $3 AS n_regionkey,
        $4 AS n_comment,
        $5 AS null_column,
        CURRENT_TIMESTAMP() AS _etl_loaded_at
    FROM @tpch_elt.staging.nation_folder
);


COPY INTO tpch_elt.staging.stg_orders 
    (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, null_column, _etl_loaded_at)
FROM (
    SELECT 
        $1 AS o_orderkey,
        $2 AS o_custkey,
        $3 AS o_orderstatus,
        $4 AS o_totalprice,
        $5 AS o_orderdate,
        $6 AS o_orderpriority,
        $7 AS o_clerk,
        $8 AS o_shippriority,
        $9 AS o_comment,
        $10 AS null_column,
        CURRENT_TIMESTAMP() AS _etl_loaded_at
    FROM @tpch_elt.staging.orders_folder
);


COPY INTO tpch_elt.staging.stg_part 
    (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, null_column, _etl_loaded_at)
FROM (
    SELECT 
        $1 AS p_partkey,
        $2 AS p_name,
        $3 AS p_mfgr,
        $4 AS p_brand,
        $5 AS p_type,
        $6 AS p_size,
        $7 AS p_container,
        $8 AS p_retailprice,
        $9 AS p_comment,
        $10 AS null_column,
        CURRENT_TIMESTAMP() AS _etl_loaded_at
    FROM @tpch_elt.staging.parts_folder
);


COPY INTO tpch_elt.staging.stg_partsupp 
    (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment, null_column, _etl_loaded_at)
FROM (
    SELECT 
        $1 AS ps_partkey,
        $2 AS ps_suppkey,
        $3 AS ps_availqty,
        $4 AS ps_supplycost,
        $5 AS ps_comment,
        $6 AS null_column,
        CURRENT_TIMESTAMP() AS _etl_loaded_at
    FROM @tpch_elt.staging.partsuppliers_folder
);


COPY INTO tpch_elt.staging.stg_suppliers 
    (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment, null_column, _etl_loaded_at)
FROM (
    SELECT 
        $1 AS s_suppkey,
        $2 AS s_name,
        $3 AS s_address,
        $4 AS s_nationkey,
        $5 AS s_phone,
        $6 AS s_acctbal,
        $7 AS s_comment,
        $8 AS null_column,
        CURRENT_TIMESTAMP() AS _etl_loaded_at
    FROM @tpch_elt.staging.suppliers_folder
);


select count(*) from tpch_elt.staging.stg_customer;




