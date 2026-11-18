{#
  Grain: one row per store x department x week (week ending Friday).
  Incremental: only silver rows changed since the last run are merged, so weekly batches and
  late corrections are processed without rebuilding three years of history.
#}
{{
    config(
        materialized='incremental',
        unique_key=['store_id', 'dept_id', 'week_date'],
        incremental_strategy=incremental_strategy(),
        on_schema_change='append_new_columns',
        liquid_clustered_by=['store_id', 'week_date'],
    )
}}

with sales as (
    select * from {{ ref('stg_walmart__weekly_sales') }}
    {% if is_incremental() %}
    where _silver_updated_at > (select max(_silver_updated_at) from {{ this }})
    {% endif %}
),

features as (
    select * from {{ ref('int_store_features_filled') }}
),

calendar as (
    select date_day, fiscal_year, fiscal_quarter, holiday_name from {{ ref('dim_date') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['s.store_id', 's.dept_id', 's.week_date']) }} as weekly_sales_sk,
    s.store_id,
    s.dept_id,
    s.week_date,
    c.fiscal_year,
    c.fiscal_quarter,
    s.weekly_sales,
    s.weekly_sales < 0 as is_net_return_week,
    s.is_holiday,
    c.holiday_name,
    f.temperature_f,
    f.fuel_price,
    f.cpi,
    f.unemployment_rate,
    f.markdowns_tracked,
    f.markdown_total as store_markdown_total,
    s._silver_updated_at
from sales as s
left join features as f
    on s.store_id = f.store_id
    and s.week_date = f.week_date
left join calendar as c
    on s.week_date = c.date_day
