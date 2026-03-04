{{ config(
    materialized='incremental',
    table_type='iceberg',
    format='parquet',
    incremental_strategy='merge',
    unique_key='officecode',
    external_location='s3://datalake-2-silver-us-east-1-086997587178-sandbox-dlh/commercial/erp/offices/',
    table_properties={
      'optimize_rewrite_delete_file_threshold': '2',
      'vacuum_max_snapshot_age_seconds': '259200'
    },
    alias='offices'
) }}

with source as (
  select
    officecode,
    city,
    phone,
    addressline1,
    addressline2,
    state,
    country,
    postalcode,
    territory,
    zone_area,
    business_area,
    source_system,
    dataset,
    extract_at
  from (
    select
      cast(b.officecode as bigint) as officecode,
      b.city,
      b.phone,
      b.addressline1,
      b.addressline2,
      b.state,
      b.country,
      b.postalcode,
      b.territory,
      'silver' as zone_area,
      'commercial' as business_area,
      'erp' as source_system,
      'offices' as dataset,
      date_parse(b.extract_at, '%Y-%m-%d %H:%i:%s.%f') - interval '3' hour as extract_at,
      row_number() over (
        partition by b.officecode
        order by date_parse(b.extract_at, '%Y-%m-%d %H:%i:%s.%f') desc
      ) as rn
    from raw.offices b
    where b.op in ('I', 'U')
    {% if is_incremental() %}
      and date_parse(b.extract_at, '%Y-%m-%d %H:%i:%s.%f') > (select max(extract_at) from ods.offices)
    {% endif %}
  ) t
  where rn = 1
)
select
  officecode,
  city,
  phone,
  addressline1,
  addressline2,
  state,
  country,
  postalcode,
  territory,
  zone_area,
  business_area,
  source_system,
  dataset,
  extract_at
from source