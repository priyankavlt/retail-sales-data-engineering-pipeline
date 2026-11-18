with history as (
    select * from {{ ref('stores_snapshot') }}
),

current_version as (
    select * from history where dbt_valid_to is null
),

versions as (
    select store_id, count(*) as version_count, min(dbt_valid_from) as first_seen_at
    from history
    group by store_id
)

select
    c.store_id,
    c.store_type,
    case c.store_type
        when 'A' then 'Supercenter'
        when 'B' then 'Discount Store'
        when 'C' then 'Neighborhood Market'
    end as store_format,
    c.store_size_sqft,
    case
        when c.store_size_sqft >= 180000 then 'XL (180k+ sqft)'
        when c.store_size_sqft >= 120000 then 'L (120-180k sqft)'
        when c.store_size_sqft >= 60000 then 'M (60-120k sqft)'
        else 'S (<60k sqft)'
    end as size_band,
    v.version_count > 1 as has_been_remodeled,
    v.version_count,
    c.dbt_valid_from as current_version_since,
    v.first_seen_at
from current_version as c
inner join versions as v on c.store_id = v.store_id
