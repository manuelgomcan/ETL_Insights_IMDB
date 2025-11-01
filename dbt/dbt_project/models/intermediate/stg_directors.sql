{{
    config(
        materialized='incremental',
        unique_key='movie_id || person_name',
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns',
        tags=['intermediate', 'directors']
    )
}}

/*
    Tabla staging para directores de películas
*/

with directors_data as (
    select distinct
        id as movie_id,
        trim(unnest(string_to_array(director, ','))) as person_name,
        _dlt_load_id
    from {{ ref('tmdb_movies_raw') }}
    where director is not null
)

select
    movie_id,
    person_name,
    _dlt_load_id
from directors_data
where person_name != ''
{% if is_incremental() %}
    and (_dlt_load_id > (select max(_dlt_load_id) from {{ this }})
        or (movie_id, person_name) not in (select movie_id, person_name from {{ this }}))
{% endif %}
order by movie_id, person_name
