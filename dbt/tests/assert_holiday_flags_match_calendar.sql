{# Source IsHoliday flags must agree with the official holiday-week calendar. #}
select f.store_id, f.dept_id, f.week_date, f.is_holiday, f.holiday_name
from {{ ref('fct_weekly_sales') }} as f
where f.is_holiday <> (f.holiday_name is not null)
