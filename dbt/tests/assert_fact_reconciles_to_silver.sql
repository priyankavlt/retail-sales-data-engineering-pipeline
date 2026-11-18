{#
  Completeness / accuracy: gold must contain exactly the sales recorded in silver.
  Returns a row (= failure) if row counts or total sales drift.
#}
with silver as (
    select count(*) as row_count, sum(weekly_sales) as total_sales
    from {{ ref('stg_walmart__weekly_sales') }}
),

gold as (
    select count(*) as row_count, sum(weekly_sales) as total_sales
    from {{ ref('fct_weekly_sales') }}
)

select
    silver.row_count as silver_rows,
    gold.row_count as gold_rows,
    silver.total_sales as silver_sales,
    gold.total_sales as gold_sales
from silver
cross join gold
where silver.row_count <> gold.row_count
   or abs(silver.total_sales - gold.total_sales) > 0.01
