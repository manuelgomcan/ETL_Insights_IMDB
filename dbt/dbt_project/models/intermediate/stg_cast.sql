{{
    config(
        materialized='incremental',
        unique_key='movie_id || person_name',
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns',
        tags=['intermediate', 'cast']
    )
}}

/*
    Tabla staging para el reparto de las películas
*/

with cast_base as (
    select distinct
        id as movie_id,
        trim(unnest(string_to_array(movie_cast, ','))) as person_name,
        'actor' as role_type,
        _dlt_load_id
    from {{ ref('tmdb_movies_raw') }}
    where movie_cast is not null
        and movie_cast != ''
)

select
    movie_id,
    person_name,
    role_type,
    _dlt_load_id
from cast_base
{% if is_incremental() %}
    where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
        or (movie_id, person_name) not in (select movie_id, person_name from {{ this }})
{% endif %}
