{% snapshot directors_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='director_name',
    strategy='check',
    check_cols=['director_name'],
    invalidate_hard_deletes=True
  )
}}

select * from {{ ref('dim_directors') }}

{% endsnapshot %}
