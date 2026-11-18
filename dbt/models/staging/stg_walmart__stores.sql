select
    store_id,
    store_type,
    store_size_sqft,
    _silver_updated_at
from {{ source('silver', 'stores') }}
