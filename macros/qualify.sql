{% macro qualify(func) %}
    {% set client_name = 'surveymonkey' %}
    {% if target.type == 'redshift' %}
        {{ func }}
    {% elif target.type == 'bigquery' %}
        {{ func|replace(client_name, "`pmg-datawarehouse`." ~ client_name) }}
    {% endif %}
{% endmacro %}
