{% snapshot movies_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='movie_id',
    strategy='check',
    check_cols=['title', 'movie_status', 'runtime', 'overview', 'tagline'],
    invalidate_hard_deletes=true
  )
}}

select * from {{ ref('dim_movies') }}

{% endsnapshot %}
