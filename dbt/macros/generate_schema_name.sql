{#
  Use custom schema names verbatim (staging / intermediate / gold / reference) instead of
  dbt's default "<target_schema>_<custom>" concatenation, so every environment exposes the
  same medallion schema names inside its own catalog/database.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
