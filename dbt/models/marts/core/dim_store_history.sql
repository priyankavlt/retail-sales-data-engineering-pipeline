{# Every version of every store, for point-in-time (as-was) reporting. #}
select
    {{ dbt_utils.generate_surrogate_key(['store_id', 'dbt_valid_from']) }} as store_version_sk,
    store_id,
    store_type,
    store_size_sqft,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('stores_snapshot') }}
