{{ config(
    materialized='incremental',
    table_type='iceberg',
    format='parquet',
    incremental_strategy='merge',
    unique_key='ordernumber',
    external_location='s3://datalake-2-silver-us-east-1-086997587178-sandbox-dlh/commercial/erp/orders/',
    table_properties={
      'optimize_rewrite_delete_file_threshold': '2',
      'vacuum_max_snapshot_age_seconds': '259200'
    },
    alias='orders'
) }}

with source as (
  select
    ordernumber,
    orderdate,
    orderyearmonth,
    customernumber,
    zone_area,
    business_area,
    source_system,
    dataset,
    extract_at
  from (
    select
      cast(b.ordernumber as bigint) as ordernumber,
      b.orderdate - interval '3' hour as orderdate,
      cast(date_format(
        b.orderdate - interval '3' hour,
        '%Y%m'
      ) as int) as orderyearmonth,
      b.customernumber,
      'silver' as zone_area,
      'commercial' as business_area,
      'erp' as source_system,
      'orders' as dataset,
      date_parse(b.extract_at, '%Y-%m-%d %H:%i:%s.%f') - interval '3' hour as extract_at,
      row_number() over (
        partition by b.ordernumber
        order by date_parse(b.extract_at, '%Y-%m-%d %H:%i:%s.%f') desc
      ) as rn
    from raw.orders b
    where b.op in ('I', 'U', 'D')
    {% if is_incremental() %}
      and date_parse(b.extract_at, '%Y-%m-%d %H:%i:%s.%f') > (select max(extract_at) from ods.orders)
    {% endif %}
  ) t
  where rn = 1
)
select
  ordernumber,
  orderdate,
  orderyearmonth,
  customernumber,
  zone_area,
  business_area,
  source_system,
  dataset,
  extract_at
from source
