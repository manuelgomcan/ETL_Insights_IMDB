{% snapshot actors_snapshot %}  

{{    
    config(
        target_schema='snapshots',
        unique_key='actor_name',
        strategy='check',
        check_cols=['actor_name'],
        invalidate_hard_deletes=True
    )
}}  

select * from {{ ref('dim_actors') }}

{% endsnapshot %}
