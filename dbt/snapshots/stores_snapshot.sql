{#
  SCD Type 2 history of store attributes. A remodel that changes a store's size or format
  closes the previous version (dbt_valid_to) and opens a new one.
#}
{% snapshot stores_snapshot %}
{{
    config(
        unique_key='store_id',
        strategy='check',
        check_cols=['store_type', 'store_size_sqft'],
        hard_deletes="invalidate",
    )
}}
select store_id, store_type, store_size_sqft from {{ ref('stg_walmart__stores') }}
{% endsnapshot %}
