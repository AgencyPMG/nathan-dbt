{% macro time_current_zone() %}
  {% if target.type == 'redshift' %}
       CONVERT_TIMEZONE('America/Chicago',sysdate)
  {% elif target.type == 'bigquery' %}
       TIMESTAMP(DATETIME(CURRENT_TIMESTAMP(),"America/Chicago"))
  {% endif %}
{% endmacro %}
