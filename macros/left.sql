{% macro left(string_text,pos_val) %}
    {% if target.type == 'bigquery' %}
        SUBSTR({{string_text}},0,{{pos_val}})
    {% elif target.type == 'redshift' %}
        LEFT({{string_text}},{{pos_val}})
    {% endif %}
{% endmacro %}