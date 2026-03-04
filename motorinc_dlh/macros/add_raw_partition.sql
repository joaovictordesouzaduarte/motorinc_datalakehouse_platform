{% macro add_raw_partition(table_name) %}
{# Machine is UTC; partition uses UTC-3 (Brazil). run_started_at - 3h when vars not provided. #}
{% set dt_utc_minus_3 = run_started_at - modules.datetime.timedelta(hours=3) %}
{# Use vars if provided, else run_started_at in UTC-3. partition_date = 'YYYY-MM-DD', partition_hour = 'HH' (00-23). #}
{% if var('partition_date', none) %}
  {% set p = var('partition_date') | string | trim %}
  {% set y = p[0:4] %}
  {% set m = p[5:7] %}
  {% set d = p[8:10] %}
{% else %}
  {% set y = var('partition_year', dt_utc_minus_3.strftime('%Y')) %}
  {% set m = var('partition_month', dt_utc_minus_3.strftime('%m')) %}
  {% set d = var('partition_day', dt_utc_minus_3.strftime('%d')) %}
{% endif %}
{% set h = var('partition_hour', dt_utc_minus_3.strftime('%H')) %}
{% set bucket = 's3://datalake-1-bronze-us-east-1-086997587178-sandbox-dlh' %}
{% set path = bucket ~ '/commercial/erp/public/' ~ table_name ~ '/' ~ y ~ '/' ~ m ~ '/' ~ d ~ '/' ~ h ~ '/' %}
ALTER TABLE raw.{{ table_name }} ADD IF NOT EXISTS PARTITION (year_stream='{{ y }}', month_stream='{{ m }}', day_stream='{{ d }}', hour_stream='{{ h }}') LOCATION '{{ path }}'
{% endmacro %}
