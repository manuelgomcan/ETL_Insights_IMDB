{% snapshot companies_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='company_name',
    strategy='check',
    check_cols=['company_name'],
    invalidate_hard_deletes=True
  )
}}

select * from {{ ref('dim_companies') }}

{% endsnapshot %}
