{{
    config(
        materialized='incremental',
        unique_key='language_name',
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns',
        tags=['intermediate', 'languages']
    )
}}

/*
    Tabla staging para idiomas hablados
*/

with languages_base as (
    select distinct
        id as movie_id,
        trim(unnest(string_to_array(spoken_languages, ','))) as language_name,
        _dlt_load_id
    from {{ ref('tmdb_movies_raw') }}
    where spoken_languages is not null
        and spoken_languages != ''
)

select
    movie_id,
    language_name,
    _dlt_load_id
from languages_base
{% if is_incremental() %}
    where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
        or language_name not in (select language_name from {{ this }})
{% endif %}
