{% macro silver_erp_location(table_name) %}
  s3://datalake-2-silver-us-east-1-086997587178-sandbox-dlh/commercial/erp/{{ table_name }}/
{% endmacro %}