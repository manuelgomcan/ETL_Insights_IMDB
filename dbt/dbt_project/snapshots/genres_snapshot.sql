{% snapshot genres_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='genre_name',
    strategy='check',
    check_cols=['genre_name'],
    invalidate_hard_deletes='true'
  )
}}

select * from {{ ref('dim_genres') }}

{% endsnapshot %}