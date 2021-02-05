{% macro split_part(string_text, delimiter_text, part_number) %}
 {% if target.type == 'redshift' %}
      split_part(
        {{string_text}},
        '{{delimiter_text}}',
        {{part_number}}
        )
  {% elif target.type == 'bigquery' %}
       split(
        {{ string_text }},
        '{{ delimiter_text }}'
        )[safe_offset({{ part_number }})]
  {% endif %}
{% endmacro %}
