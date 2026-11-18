{# Department performance per fiscal year: revenue, share of chain sales, rank and YoY growth. #}
with dept_year as (
    select
        dept_id,
        fiscal_year,
        sum(weekly_sales) as total_sales,
        count(distinct store_id) as selling_stores,
        count(distinct week_date) as weeks_reported
    from {{ ref('fct_weekly_sales') }}
    group by dept_id, fiscal_year
),

ranked as (
    select
        *,
        sum(total_sales) over (partition by fiscal_year) as chain_sales,
        rank() over (partition by fiscal_year order by total_sales desc) as sales_rank,
        lag(total_sales) over (partition by dept_id order by fiscal_year) as prior_year_sales,
        lag(weeks_reported) over (partition by dept_id order by fiscal_year) as prior_year_weeks
    from dept_year
)

select
    r.dept_id,
    d.department_name,
    d.department_group,
    r.fiscal_year,
    r.weeks_reported,
    r.selling_stores,
    r.total_sales,
    {{ dbt_utils.safe_divide('r.total_sales', 'r.weeks_reported') }} as avg_sales_per_week,
    {{ dbt_utils.safe_divide('r.total_sales', 'r.chain_sales') }} as share_of_chain_sales,
    r.sales_rank,
    -- Fiscal years are partially covered at the edges of the data, so compare weekly run-rates.
    {{ dbt_utils.safe_divide(
        dbt_utils.safe_divide('r.total_sales', 'r.weeks_reported') ~ ' - ' ~ dbt_utils.safe_divide('r.prior_year_sales', 'r.prior_year_weeks'),
        dbt_utils.safe_divide('r.prior_year_sales', 'r.prior_year_weeks')
    ) }} as yoy_run_rate_growth_pct
from ranked as r
inner join {{ ref('dim_department') }} as d on r.dept_id = d.dept_id
