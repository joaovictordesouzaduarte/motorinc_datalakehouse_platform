{{ config(
    materialized='incremental',
    table_type='iceberg',
    format='parquet',
    incremental_strategy='merge',
    unique_key=['ordernumber', 'productcode'],
    external_location='s3://datalake-2-silver-us-east-1-086997587178-sandbox-dlh/commercial/erp/orderdetails/',
    table_properties={
      'optimize_rewrite_delete_file_threshold': '2',
      'vacuum_max_snapshot_age_seconds': '259200'
    },
    alias='orderdetails'
) }}

with orders as (
  select ordernumber, orderdate
  from (
    select
      ordernumber,
      orderdate,
      row_number() over (
        partition by ordernumber
        order by date_parse(extract_at, '%Y-%m-%d %H:%i:%s.%f') desc
      ) as rn
    from raw.orders
    where op in ('I', 'U', 'D')
  ) t
  where rn = 1
),
orderdetails_latest as (
  select
    ordernumber, orderdate, productcode, quantityordered, priceeach,
    orderlinenumber, amount, zone_area, business_area, source_system, dataset, extract_at
  from (
    select
      b.ordernumber,
      o.orderdate - interval '3' hour as orderdate,
      b.productcode,
      b.quantityordered,
      cast(b.priceeach as double) as priceeach,
      b.orderlinenumber,
      cast(b.quantityordered * b.priceeach as double) as amount,
      'silver' as zone_area,
      'commercial' as business_area,
      'erp' as source_system,
      'orderdetails' as dataset,
      date_parse(b.extract_at, '%Y-%m-%d %H:%i:%s.%f') as extract_at,
      row_number() over (
        partition by b.ordernumber, b.productcode, b.orderlinenumber
        order by date_parse(b.extract_at, '%Y-%m-%d %H:%i:%s.%f') desc
      ) as rn
    from raw.orderdetails b
    left join orders o on b.ordernumber = o.ordernumber
    where b.op in ('I', 'U')
    {% if is_incremental() %}
      and date_parse(b.extract_at, '%Y-%m-%d %H:%i:%s.%f') > (select max(extract_at) from ods.orderdetails)
    {% endif %}
  ) t
  where rn = 1
)
select
  ordernumber,
  orderdate,
  productcode,
  quantityordered,
  priceeach,
  orderlinenumber,
  amount,
  zone_area,
  business_area,
  source_system,
  dataset,
  extract_at
from orderdetails_latest
