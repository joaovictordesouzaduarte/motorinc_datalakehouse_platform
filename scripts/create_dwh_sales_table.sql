
CREATE TABLE IF NOT EXISTS dwh.sales_dataset(
  locality_officecode bigint, 
  locality_city string, 
  locality_state string, 
  locality_country string, 
  locality_territory string, 
  interval_portsales string, 
  calendar_sales_date timestamp,
  calendar_month_number bigint,
  calendar_monthname_full string, 
  calendar_monthname_short string, 
  -- calendar_year_month_number bigint, 
  calendar_quarter bigint, 
  calendar_year bigint,
  sales_quantityordered bigint, 
  sales_amount double)
PARTITIONED BY ( 
  month(calendar_sales_date))
LOCATION
  's3://datalake-3-gold-us-east-1-086997587178-sandbox-dlh/dwh/commercial/sales'
TBLPROPERTIES (
  'table_type' ='ICEBERG');
  