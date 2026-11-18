select
    store_id,
    dept_id,
    week_date,
    cast(weekly_sales as decimal(14, 2)) as weekly_sales,
    is_holiday,
    _silver_updated_at
from {{ source('silver', 'weekly_sales') }}
