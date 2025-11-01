{{
    config(
        materialized='incremental',
        unique_key='genre_name',
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns'
    )
}}

with all_genres as (
    select distinct
        trim(unnest(string_to_array(genres, ','))) as genre_name,
        _dlt_load_id
    from {{ ref('tmdb_movies_raw') }}
    where genres is not null
)

select
    genre_name,
    _dlt_load_id
from all_genres
where genre_name != ''
{% if is_incremental() %}
    and (_dlt_load_id > (select max(_dlt_load_id) from {{ this }})
        or genre_name not in (select genre_name from {{ this }}))
{% endif %}
order by genre_name
