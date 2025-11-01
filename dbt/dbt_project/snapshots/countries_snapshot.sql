{% snapshot countries_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='country_name',
    strategy='check',
    check_cols=['country_name'],
    invalidate_hard_deletes=True
  )
}}

select * from {{ ref('dim_countries') }}

{% endsnapshot %}
