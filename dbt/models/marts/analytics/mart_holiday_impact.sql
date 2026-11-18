{#
  Holiday lift: how much a holiday week out-sells the same store's typical non-holiday week,
  by store format. Drives staffing and inventory planning for the four Walmart event weeks.
#}
with store_week as (
    select
        f.store_id,
        f.week_date,
        f.holiday_name,
        sum(f.weekly_sales) as total_sales
    from {{ ref('fct_weekly_sales') }} as f
    group by f.store_id, f.week_date, f.holiday_name
),

baseline as (
    select store_id, avg(total_sales) as avg_non_holiday_sales
    from store_week
    where holiday_name is null
    group by store_id
),

holiday_weeks as (
    select
        sw.store_id,
        sw.holiday_name,
        sw.week_date,
        sw.total_sales,
        b.avg_non_holiday_sales
    from store_week as sw
    inner join baseline as b on sw.store_id = b.store_id
    where sw.holiday_name is not null
)

select
    h.holiday_name,
    s.store_format,
    count(distinct h.week_date) as holiday_weeks_observed,
    count(distinct h.store_id) as stores,
    avg(h.total_sales) as avg_holiday_week_sales,
    avg(h.avg_non_holiday_sales) as avg_baseline_week_sales,
    {{ dbt_utils.safe_divide('sum(h.total_sales) - sum(h.avg_non_holiday_sales)', 'sum(h.avg_non_holiday_sales)') }} as holiday_lift_pct
from holiday_weeks as h
inner join {{ ref('dim_store') }} as s on h.store_id = s.store_id
group by h.holiday_name, s.store_format
