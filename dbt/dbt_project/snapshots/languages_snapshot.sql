{% snapshot languages_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='language_name',
    strategy='check',
    check_cols=['language_name'],
    invalidate_hard_deletes=True
  )
}}

select * from {{ ref('dim_languages') }}

{% endsnapshot %}
