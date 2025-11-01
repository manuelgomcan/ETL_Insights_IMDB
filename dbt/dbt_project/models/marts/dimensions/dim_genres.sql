{{
    config(
        materialized='incremental',
        unique_key='genre_name',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        tags=['dimensions', 'dimension'],
        meta={
            'surrogate_key': true
        }
    )
}}

with genres_data as (
    select distinct
        genre_name,
        max(_dlt_load_id) as _dlt_load_id
    from {{ ref('stg_genres') }}
    where genre_name != ''
    group by 1
)

select
    {{ dbt_utils.generate_surrogate_key(['genre_name']) }} as genre_id,
    genre_name,
    _dlt_load_id
from genres_data
{% if is_incremental() %}
where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
   or not exists (select 1 from {{ this }} where {{ this }}.genre_name = genres_data.genre_name)
{% endif %}
