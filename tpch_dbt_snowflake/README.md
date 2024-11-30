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


# ER Diagram:
![alt text](image.png)

# Snowflake database structure

### Schema structure
![alt text](image-1.png)
- staging --> all the raw data files from s3 are loaded into landing zone in snowflake
- tpch_bronze --> 
- tpch_silver --> 
- tpch_gold --> 
#### Staging schema structure
![alt text](image-2.png)
- this is a landing zone into snowflake environment
- stages and file formats are created to load csv files from s3
- storage integration method is used to create connection between s3 and snowflake
#### Bronze schema structure
![alt text](image-3.png)
- data are picked from staging and applied data cleaning transformations such as data type conversion, null handling, text cleaning (trimming extra spaces)
#### Silver schema structure
![alt text](image-4.png)
- data are ingested incrementally and applied transformations
#### Gold schema structure
![alt text](image-5.png)
- data are ingested in facts and dimensions to enable faster analytical queries

# Tech Stack used:
- S3
- Snowflake
- DBT
- Airflow

# Solution:
