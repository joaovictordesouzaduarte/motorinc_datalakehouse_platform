{{config (
    materialized='incremental',
    table_type='iceberg',
    format='parquet',
    incremental_strategy='merge',
    unique_key=['locality_officecode', 'calendar_sales_date'],
    external_location='s3://datalake-3-gold-us-east-1-086997587178-sandbox-dlh/commercial/sales/dataset/',
    table_properties={
      'optimize_rewrite_delete_file_threshold': '2',
      'vacuum_max_snapshot_age_seconds': '259200'
    },
    alias='sales_dataset'
) }}

with 
    source_orders as (
        select
            o.ordernumber ,
            o.customernumber, 
            o.orderdate,
            o.orderyearmonth,
            od.amount,
            o.extract_at,
            c.customernumber,
            c.salesrepemployeenumber,
            e.employeenumber,
            e.officecode,
            od.quantityordered
        from {{ref('stg_orderdetails')}} od
        inner join {{ref('stg_orders')}} o on od.ordernumber = o.ordernumber
        inner join {{ref('stg_customers')}} c on o.customernumber = c.customernumber
        inner join {{ref('stg_employees')}} e on c.salesrepemployeenumber = e.employeenumber
        where 1=1
        {% if is_incremental() %}
            and o.orderdate > (select max(calendar_sales_date) from dwh.sales_dataset)
        {% endif %}
        order by e.officecode
    ),
    source_offices as (
        select
            o.officecode,
            o.city,
            o.state,
            o.country,
            o.territory
        from {{ref('stg_offices')}} o
        order by o.officecode
    )

   select 
        sf.officecode as locality_officecode,
        sf.city as locality_city,
        case when sf.state = '' then null else sf.state end as locality_state,
        sf.country as locality_country,
        sf.territory as locality_territory,    
        case when so.amount <= 50000 then 'Small'
            when so.amount between 50000 and 75000 then 'Medium' else 'Big' end as interval_portsales,
        so.orderdate as calendar_sales_date,
        cast(month(so.orderdate) as bigint) as calendar_month_number,
        date_format(so.orderdate, '%M') as calendar_monthname_full,
        date_format(so.orderdate, '%m') as calendar_monthname_short,
        quarter(so.orderdate) as calendar_quarter,
        year(so.orderdate) as calendar_year,
        sum(so.quantityordered) as sales_quantityordered,
        sum(so.amount) as sales_amount
    from source_orders so
    inner join source_offices sf on so.officecode = sf.officecode
    group by sf.officecode, sf.city, sf.state, sf.country, sf.territory, case when so.amount <= 50000 then 'Small'
            when so.amount between 50000 and 75000 then 'Medium' else 'Big' end, so.orderdate

