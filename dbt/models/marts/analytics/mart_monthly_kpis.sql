{# Executive scorecard by calendar month of the sales week. #}
with weekly as (
    select
        f.week_date,
        sum(f.weekly_sales) as total_sales,
        count(distinct f.store_id) as reporting_stores,
        max(case when f.holiday_name is not null then 1 else 0 end) as is_holiday_week
    from {{ ref('fct_weekly_sales') }} as f
    group by f.week_date
),

monthly as (
    select
        d.calendar_year,
        d.calendar_month,
        max(d.month_name) as month_name,
        max(d.fiscal_year) as fiscal_year,
        count(*) as weeks,
        sum(w.total_sales) as total_sales,
        sum(case when w.is_holiday_week = 1 then w.total_sales else 0 end) as holiday_week_sales,
        max(w.reporting_stores) as reporting_stores
    from weekly as w
    inner join {{ ref('dim_date') }} as d on w.week_date = d.date_day
    group by d.calendar_year, d.calendar_month
)

select
    m.*,
    {{ dbt_utils.safe_divide('m.total_sales', 'm.weeks') }} as avg_weekly_sales,
    {{ dbt_utils.safe_divide('m.holiday_week_sales', 'm.total_sales') }} as holiday_sales_share,
    py.total_sales as prior_year_sales,
    -- Months contain 4 or 5 sales weeks, so compare weekly run-rates rather than raw totals.
    {{ dbt_utils.safe_divide(
        dbt_utils.safe_divide('m.total_sales', 'm.weeks') ~ ' - ' ~ dbt_utils.safe_divide('py.total_sales', 'py.weeks'),
        dbt_utils.safe_divide('py.total_sales', 'py.weeks')
    ) }} as yoy_run_rate_growth_pct
from monthly as m
left join monthly as py
    on m.calendar_month = py.calendar_month
    and m.calendar_year = py.calendar_year + 1
