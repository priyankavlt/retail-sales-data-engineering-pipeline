with departments as (
    select
        dept_id,
        count(distinct store_id) as stores_carrying,
        min(week_date) as first_sales_week,
        max(week_date) as last_sales_week
    from {{ ref('stg_walmart__weekly_sales') }}
    group by dept_id
)

select
    d.dept_id,
    coalesce(n.department_name, 'Department ' || cast(d.dept_id as varchar(8))) as department_name,
    coalesce(n.department_group, 'Unmapped') as department_group,
    d.stores_carrying,
    d.first_sales_week,
    d.last_sales_week
from departments as d
left join {{ ref('department_names') }} as n on d.dept_id = n.dept_id
