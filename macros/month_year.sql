{% macro month_year(date) %}
    {% if target.type == 'bigquery' %}
        FORMAT_TIMESTAMP("%m-%Y", CAST({{date}} AS timestamp))
    {% elif target.type == 'redshift' %}
        to_char({{date}}::timestamp,'MM-YYYY')
    {% endif %}
{% endmacro %}