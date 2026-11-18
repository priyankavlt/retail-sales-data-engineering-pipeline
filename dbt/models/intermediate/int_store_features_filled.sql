{#
  Store-week context ready for analysis:
  * CPI and unemployment are published with gaps -> forward-fill the last known value per store.
  * Markdowns before tracking began are unknown (NULL); afterwards a NULL means "no markdown".
#}
with features as (
    select * from {{ ref('stg_walmart__store_features') }}
),

filled as (
    select
        store_id,
        week_date,
        temperature_f,
        fuel_price,
        {{ last_value_ignore_nulls('cpi', 'store_id', 'week_date') }} as cpi,
        {{ last_value_ignore_nulls('unemployment_rate', 'store_id', 'week_date') }} as unemployment_rate,
        week_date >= cast('{{ var("markdown_tracking_start") }}' as date) as markdowns_tracked,
        markdown_1,
        markdown_2,
        markdown_3,
        markdown_4,
        markdown_5,
        is_holiday
    from features
)

select
    store_id,
    week_date,
    temperature_f,
    fuel_price,
    cpi,
    unemployment_rate,
    markdowns_tracked,
    case
        when markdowns_tracked then
            coalesce(markdown_1, 0) + coalesce(markdown_2, 0) + coalesce(markdown_3, 0)
            + coalesce(markdown_4, 0) + coalesce(markdown_5, 0)
    end as markdown_total,
    case
        when markdowns_tracked then
            (case when coalesce(markdown_1, 0) <> 0 then 1 else 0 end)
            + (case when coalesce(markdown_2, 0) <> 0 then 1 else 0 end)
            + (case when coalesce(markdown_3, 0) <> 0 then 1 else 0 end)
            + (case when coalesce(markdown_4, 0) <> 0 then 1 else 0 end)
            + (case when coalesce(markdown_5, 0) <> 0 then 1 else 0 end)
    end as active_markdown_count,
    is_holiday
from filled
