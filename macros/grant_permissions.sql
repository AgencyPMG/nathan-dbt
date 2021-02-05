{% macro grant_permissions(schemas, target_name, target_schema) %}
    {% if target_name == 'redshift-dev' or target_name == 'redshift-prod' %}
        {% for schema in schemas %}
            {% set client_group = 'analytics' %}
            grant usage on schema {{ schema }} to group {{ client_group }} ;
            grant select on all tables in schema {{ schema }} to group {{ client_group }};
        {% endfor %}
    {% endif %}
{% endmacro %}
