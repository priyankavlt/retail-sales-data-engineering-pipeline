{#
  Daily calendar with Walmart's fiscal calendar (fiscal year ends 31 January, so FY2012 runs
  1 Feb 2011 - 31 Jan 2012) and the week-ending-Friday key used by all sales facts.
#}
{% set months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August',
                 'September', 'October', 'November', 'December'] %}
{% set days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'] %}

with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ var('date_spine_start') ~ "' as date)",
        end_date="cast('" ~ var('date_spine_end') ~ "' as date)"
    ) }}
),

base as (
    select
        cast(date_day as date) as date_day,
        {{ day_of_week_iso('cast(date_day as date)') }} as iso_day_of_week
    from spine
),

calendar as (
    select
        date_day,
        iso_day_of_week,
        cast(extract(year from date_day) as integer) as calendar_year,
        cast(extract(quarter from date_day) as integer) as calendar_quarter,
        cast(extract(month from date_day) as integer) as calendar_month,
        cast(extract(day from date_day) as integer) as day_of_month,
        cast(extract(week from date_day) as integer) as iso_week_of_year,
        cast({{ dbt.dateadd('day', '(12 - iso_day_of_week) % 7', 'date_day') }} as date) as week_ending_date
    from base
)

select
    c.date_day,
    c.calendar_year,
    c.calendar_quarter,
    c.calendar_month,
    case c.calendar_month
        {% for m in months %}when {{ loop.index }} then '{{ m }}' {% endfor %}
    end as month_name,
    c.day_of_month,
    c.iso_day_of_week,
    case c.iso_day_of_week
        {% for d in days %}when {{ loop.index }} then '{{ d }}' {% endfor %}
    end as day_name,
    c.iso_day_of_week in (6, 7) as is_weekend,
    c.iso_week_of_year,
    c.week_ending_date,
    c.calendar_year + case when c.calendar_month >= 2 then 1 else 0 end as fiscal_year,
    case
        when c.calendar_month in (2, 3, 4) then 1
        when c.calendar_month in (5, 6, 7) then 2
        when c.calendar_month in (8, 9, 10) then 3
        else 4
    end as fiscal_quarter,
    h.holiday_name,
    h.holiday_name is not null as is_holiday_week
from calendar as c
left join {{ ref('holiday_weeks') }} as h on c.week_ending_date = h.week_date
