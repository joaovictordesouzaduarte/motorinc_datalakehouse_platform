{{ config(
    materialized='incremental',
    table_type='iceberg',
    format='parquet',
    incremental_strategy='merge',
    unique_key='customernumber',
    external_location='s3://datalake-2-silver-us-east-1-086997587178-sandbox-dlh/commercial/erp/customers/',
    table_properties={
      'optimize_rewrite_delete_file_threshold': '2'
    },
    alias='customers'
) }}

with source as (
  select
    customernumber,
    customername,
    contactlastname,
    contactfirstname,
    phone,
    addressline1,
    addressline2,
    city,
    state,
    postalcode,
    country,
    salesrepemployeenumber,
    creditlimit,
    zone_area,
    business_area,
    source_system,
    dataset,
    extract_at
  from (
    select
      b.customernumber,
      b.customername,
      b.contactlastname,
      b.contactfirstname,
      b.phone,
      b.addressline1,
      b.addressline2,
      b.city,
      b.state,
      b.postalcode,
      b.country,
      case when b.salesrepemployeenumber is null then 0 else b.salesrepemployeenumber end as salesrepemployeenumber,
      b.creditlimit,
      'silver' as zone_area,
      'commercial' as business_area,
      'erp' as source_system,
      'customers' as dataset,
      date_parse(b.extract_at, '%Y-%m-%d %H:%i:%s.%f') - interval '3' hour as extract_at,
      row_number() over (
        partition by b.customernumber
        order by date_parse(b.extract_at, '%Y-%m-%d %H:%i:%s.%f') desc
      ) as rn
    from raw.customers b
    where b.op in ('I', 'U')
    {% if is_incremental() %}
      and date_parse(b.extract_at, '%Y-%m-%d %H:%i:%s.%f') > (select max(extract_at) from ods.customers)
    {% endif %}
  ) t
  where rn = 1
)
select
  customernumber,
  customername,
  contactlastname,
  contactfirstname,
  phone,
  addressline1,
  addressline2,
  city,
  state,
  postalcode,
  country,
  salesrepemployeenumber,
  creditlimit,
  zone_area,
  business_area,
  source_system,
  dataset,
  extract_at
from source
