-- Ad-hoc: top 10 stores by sales over the most recent 52 weeks, with YoY growth.
with bounds as (
    select max(week_date) as last_week from {{ ref('mart_store_weekly_performance') }}
)
select
    p.store_id,
    p.store_format,
    sum(p.total_sales) as sales_52w,
    sum(p.same_week_last_year_sales) as sales_prior_52w,
    {{ dbt_utils.safe_divide('sum(p.total_sales) - sum(p.same_week_last_year_sales)', 'sum(p.same_week_last_year_sales)') }} as yoy_growth_pct
from {{ ref('mart_store_weekly_performance') }} as p
cross join bounds as b
where p.week_date > {{ dbt.dateadd('week', -52, 'b.last_week') }}
group by p.store_id, p.store_format
order by sales_52w desc
limit 10
