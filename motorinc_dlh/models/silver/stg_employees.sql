{{ config(
    materialized='incremental',
    table_type='iceberg',
    format='parquet',
    incremental_strategy='merge',
    unique_key='employeenumber',
    external_location='s3://datalake-2-silver-us-east-1-086997587178-sandbox-dlh/commercial/erp/employees/',
    table_properties={
      'optimize_rewrite_delete_file_threshold': '2',
      'vacuum_max_snapshot_age_seconds': '259200'
    },
    alias='employees'
) }}

with source as (
  select
    b.employeenumber,
    b.lastname,
    b.firstname,
    b.extension,
    b.email,
    cast(b.officecode as bigint) as officecode,
    coalesce(b.reportsto, 0) as reportsto,
    b.jobtitle,
    'silver' as zone_area,
    'commercial' as business_area,
    'erp' as source_system,
    'employees' as dataset,
    date_parse(b.extract_at, '%Y-%m-%d %H:%i:%s.%f') - interval '3' hour as extract_at
  from raw.employees b
  where b.op in ('I', 'U')
  {% if is_incremental() %}
    and date_parse(b.extract_at, '%Y-%m-%d %H:%i:%s.%f') > (select max(extract_at) from ods.employees)
  {% endif %}
)

select
  employeenumber,
  lastname,
  firstname,
  extension,
  email,
  officecode,
  reportsto,
  jobtitle,
  zone_area,
  business_area,
  source_system,
  dataset,
  extract_at
from source