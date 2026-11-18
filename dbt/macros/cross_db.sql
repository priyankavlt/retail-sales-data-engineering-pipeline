{#
  Thin cross-database helpers so the same models run on DuckDB (local/CI) and Databricks
  (production). dbt-databricks resolves spark__ implementations.
#}

{# ISO day of week: Monday = 1 ... Sunday = 7 #}
{% macro day_of_week_iso(date_expr) -%}
    {{ return(adapter.dispatch('day_of_week_iso', 'walmart_analytics')(date_expr)) }}
{%- endmacro %}

{% macro default__day_of_week_iso(date_expr) -%}
    cast(extract(isodow from {{ date_expr }}) as integer)
{%- endmacro %}

{% macro spark__day_of_week_iso(date_expr) -%}
    (weekday({{ date_expr }}) + 1)
{%- endmacro %}


{# Forward-fill: last non-null value up to and including the current row. #}
{% macro last_value_ignore_nulls(column, partition_by, order_by) -%}
    {{ return(adapter.dispatch('last_value_ignore_nulls', 'walmart_analytics')(column, partition_by, order_by)) }}
{%- endmacro %}

{% macro default__last_value_ignore_nulls(column, partition_by, order_by) -%}
    last_value({{ column }} ignore nulls) over (
        partition by {{ partition_by }} order by {{ order_by }}
        rows between unbounded preceding and current row
    )
{%- endmacro %}

{% macro spark__last_value_ignore_nulls(column, partition_by, order_by) -%}
    last({{ column }}, true) over (
        partition by {{ partition_by }} order by {{ order_by }}
        rows between unbounded preceding and current row
    )
{%- endmacro %}


{# Incremental strategy supported by each adapter. #}
{% macro incremental_strategy() -%}
    {{ return('merge' if target.type == 'databricks' else 'delete+insert') }}
{%- endmacro %}
