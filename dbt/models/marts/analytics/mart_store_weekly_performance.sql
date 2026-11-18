{# Store-week KPIs: totals, sales density, week-over-week, year-over-year and 4-week trend. #}
with store_week as (
    select
        store_id,
        week_date,
        max(fiscal_year) as fiscal_year,
        max(holiday_name) as holiday_name,
        sum(weekly_sales) as total_sales,
        count(distinct dept_id) as active_departments,
        sum(case when is_net_return_week then 1 else 0 end) as return_departments
    from {{ ref('fct_weekly_sales') }}
    group by store_id, week_date
),

with_trends as (
    select
        sw.*,
        lag(total_sales, 1) over (partition by store_id order by week_date) as prior_week_sales,
        lag(total_sales, 52) over (partition by store_id order by week_date) as same_week_last_year_sales,
        avg(total_sales) over (
            partition by store_id order by week_date rows between 3 preceding and current row
        ) as rolling_4wk_avg_sales,
        rank() over (partition by week_date order by total_sales desc) as sales_rank_in_week
    from store_week as sw
)

select
    t.store_id,
    s.store_type,
    s.store_format,
    t.week_date,
    t.fiscal_year,
    t.holiday_name,
    t.total_sales,
    t.active_departments,
    t.return_departments,
    {{ dbt_utils.safe_divide('t.total_sales', 's.store_size_sqft') }} as sales_per_sqft,
    t.prior_week_sales,
    {{ dbt_utils.safe_divide('t.total_sales - t.prior_week_sales', 't.prior_week_sales') }} as wow_growth_pct,
    t.same_week_last_year_sales,
    {{ dbt_utils.safe_divide('t.total_sales - t.same_week_last_year_sales', 't.same_week_last_year_sales') }} as yoy_growth_pct,
    t.rolling_4wk_avg_sales,
    t.sales_rank_in_week
from with_trends as t
inner join {{ ref('dim_store') }} as s on t.store_id = s.store_id
