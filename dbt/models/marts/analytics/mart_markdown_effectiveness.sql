{#
  Promotional markdown effectiveness. Each store-week is compared with that store's own average
  non-holiday week (markdown-tracked period only), so store size differences do not masquerade
  as promotion lift. Holiday weeks are excluded so event peaks are not credited to markdowns.
  This is descriptive, not causal: markdown depth is also correlated with season.
#}
with store_week as (
    select
        store_id,
        week_date,
        max(holiday_name) as holiday_name,
        max(store_markdown_total) as markdown_total,
        sum(weekly_sales) as total_sales
    from {{ ref('fct_weekly_sales') }}
    where markdowns_tracked
    group by store_id, week_date
),

banded as (
    select
        *,
        case
            when markdown_total = 0 then '0: none'
            when markdown_total < 5000 then '1: under $5k'
            when markdown_total < 20000 then '2: $5k-$20k'
            when markdown_total < 50000 then '3: $20k-$50k'
            else '4: $50k+'
        end as markdown_band
    from store_week
    where holiday_name is null  -- keep holiday peaks from inflating promotion lift
),

store_baseline as (
    select
        store_id,
        avg(total_sales) as baseline_sales
    from banded
    group by store_id
)

select
    s.store_format,
    b.markdown_band,
    count(*) as store_weeks,
    avg(b.markdown_total) as avg_markdown_spend,
    avg(b.total_sales) as avg_weekly_sales,
    {{ dbt_utils.safe_divide('sum(b.total_sales) - sum(sb.baseline_sales)', 'sum(sb.baseline_sales)') }} as sales_lift_vs_store_avg_pct,
    {{ dbt_utils.safe_divide('sum(b.total_sales) - sum(sb.baseline_sales)', 'sum(b.markdown_total)') }} as incremental_sales_per_markdown_dollar
from banded as b
inner join store_baseline as sb on b.store_id = sb.store_id
inner join {{ ref('dim_store') }} as s on b.store_id = s.store_id
group by s.store_format, b.markdown_band
