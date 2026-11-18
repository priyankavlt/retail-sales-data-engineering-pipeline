select
    store_id,
    week_date,
    temperature_f,
    fuel_price,
    markdown_1,
    markdown_2,
    markdown_3,
    markdown_4,
    markdown_5,
    cpi,
    unemployment_rate,
    is_holiday,
    _silver_updated_at
from {{ source('silver', 'store_features') }}
