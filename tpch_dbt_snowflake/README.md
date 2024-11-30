# Introduction


# Requirements


# Datasets
- customers
- line item
- Nation
- Orders
- Part
- Parts Supplier
- Region
- Supplier

### Facts
- orders
- line items
### Dimension
- customers
- Nation
- Part
- Parts Supplier
- Region
- Supplier

# ER Diagram:
![alt text](image.png)

# Snowflake database structure

### Schema structure
![alt text](image-1.png)
- staging --> all the raw data files from s3 are loaded into landing zone in snowflake
- tpch_bronze --> files are picked and applied data cleaning transformations such as data type conversion, null handling, text cleaning (trimming extra spaces)
- tpch_silver --> data are ingested incrementally and applied transformations
- tpch_gold --> data are stored in dimensional modelling fashion to enable fast analytical queries
### Staging schema structure
![alt text](image-2.png)
- this is a landing zone into snowflake environment
- stages and file formats are created to load csv files from s3
- storage integration method is used to create connection between s3 and snowflake

# Tech Stack used:
- S3
- Snowflake
- DBT
- Airflow

# Solution:
