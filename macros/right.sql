{% macro right(string_text,pos_val) %}
    {% if target.type == 'bigquery' %}
        SUBSTR({{string_text}},-{{pos_val}})
    {% elif target.type == 'redshift' %}
        RIGHT({{string_text}},{{pos_val}})
    {% endif %}
{% endmacro %}