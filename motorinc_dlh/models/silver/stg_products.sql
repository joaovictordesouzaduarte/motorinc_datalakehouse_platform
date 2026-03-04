{{ config(
    materialized='incremental',
    table_type='iceberg',
    format='parquet',
    incremental_strategy='merge',
    unique_key='productcode',
    external_location='s3://datalake-2-silver-us-east-1-086997587178-sandbox-dlh/commercial/erp/products/',
    table_properties={
      'optimize_rewrite_delete_file_threshold': '2',
      'vacuum_max_snapshot_age_seconds': '259200'
    },
    alias='products'
) }}

with source as (
  select
    productcode,
    productname,
    productline,
    productfamily,
    productscale,
    productvendor,
    productdescription,
    quantityinstock,
    buyprice,
    msrp,
    zone_area,
    business_area,
    source_system,
    dataset,
    extract_at
  from (
    select
      b.productcode,
      b.productname,
      b.productline,
      b.productfamily,
      b.productscale,
      b.productvendor,
      b.productdescription,
      b.quantityinstock,
      cast(b.buyprice as double) as buyprice,
      cast(b.msrp as double) as msrp,
      'silver' as zone_area,
      'commercial' as business_area,
      'erp' as source_system,
      'products' as dataset,
      date_parse(b.extract_at, '%Y-%m-%d %H:%i:%s.%f') - interval '3' hour as extract_at,
      row_number() over (
        partition by b.productcode
        order by date_parse(b.extract_at, '%Y-%m-%d %H:%i:%s.%f') desc
      ) as rn
    from raw.products b
    where b.op in ('I', 'U')
    {% if is_incremental() %}
      and date_parse(b.extract_at, '%Y-%m-%d %H:%i:%s.%f') > (select max(extract_at) from ods.products)
    {% endif %}
  ) t
  where rn = 1
)
select
  productcode,
  productname,
  productline,
  productfamily,
  productscale,
  productvendor,
  productdescription,
  quantityinstock,
  buyprice,
  msrp,
  zone_area,
  business_area,
  source_system,
  dataset,
  extract_at
from source
