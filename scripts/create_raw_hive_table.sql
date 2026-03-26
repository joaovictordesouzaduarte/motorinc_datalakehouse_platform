CREATE EXTERNAL TABLE IF NOT EXISTS raw.customers(
  op string,
  extract_at string,
  customernumber bigint, 
  customername string, 
  contactlastname string, 
  contactfirstname string, 
  phone string, 
  addressline1 string, 
  addressline2 string, 
  city string, 
  `state` string, 
  postalcode string, 
  country string, 
  salesrepemployeenumber bigint, 
  creditlimit decimal(18,2))
PARTITIONED BY ( 
  year_stream string, 
  month_stream string, 
  day_stream string, 
  hour_stream string)  
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://datalake-1-bronze-us-east-1-086997587178-sandbox-dlh/commercial/erp/public/customers/'
TBLPROPERTIES (
  'classification'='parquet');

CREATE EXTERNAL TABLE IF NOT EXISTS raw.employees(
  op string, 
  extract_at string,
  employeenumber bigint, 
  lastname string, 
  firstname string, 
  extension string, 
  email string, 
  officecode string, 
  reportsto bigint, 
  jobtitle string)
PARTITIONED BY ( 
  year_stream string, 
  month_stream string, 
  day_stream string, 
  hour_stream string)    
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://datalake-1-bronze-us-east-1-086997587178-sandbox-dlh/commercial/erp/public/employees/'
TBLPROPERTIES (
  'classification'='parquet');

CREATE EXTERNAL TABLE IF NOT EXISTS raw.offices(
  op string, 
  extract_at string,  
  officecode string, 
  city string, 
  phone string, 
  addressline1 string, 
  addressline2 string, 
  `state` string, 
  country string, 
  postalcode string, 
  territory string)
PARTITIONED BY ( 
  year_stream string, 
  month_stream string, 
  day_stream string, 
  hour_stream string)     
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://datalake-1-bronze-us-east-1-086997587178-sandbox-dlh/commercial/erp/public/offices'
TBLPROPERTIES (
  'classification'='parquet');

CREATE EXTERNAL TABLE IF NOT EXISTS raw.orderdetails(
  op string, 
  extract_at string,    
  ordernumber bigint, 
  productcode bigint, 
  quantityordered bigint, 
  priceeach decimal(18,2),
  orderlinenumber bigint)
PARTITIONED BY ( 
  year_stream string, 
  month_stream string, 
  day_stream string, 
  hour_stream string)    
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://datalake-1-bronze-us-east-1-086997587178-sandbox-dlh/commercial/erp/public/orderdetails'
TBLPROPERTIES (
  'classification'='parquet');

CREATE EXTERNAL TABLE IF NOT EXISTS raw.orders(
  op string, 
  extract_at string,    
  ordernumber bigint, 
  orderdate timestamp, 
  requirededate timestamp,
  shippeddate timestamp,
  customernumber bigint)
PARTITIONED BY ( 
  year_stream string, 
  month_stream string, 
  day_stream string, 
  hour_stream string)      
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://datalake-1-bronze-us-east-1-086997587178-sandbox-dlh/commercial/erp/public/orders'
TBLPROPERTIES (
  'classification'='parquet');

CREATE EXTERNAL TABLE IF NOT EXISTS raw.products(
  op string, 
  extract_at string,    
  productcode bigint, 
  productname string, 
  productline string, 
  productfamily string, 
  productscale string, 
  productvendor string, 
  productdescription string, 
  quantityinstock bigint, 
  buyprice decimal(18,2), 
  msrp decimal(18,2))
PARTITIONED BY ( 
  year_stream string, 
  month_stream string, 
  day_stream string, 
  hour_stream string)     
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://datalake-1-bronze-us-east-1-086997587178-sandbox-dlh/commercial/erp/public/products'
TBLPROPERTIES (
  'classification'='parquet');