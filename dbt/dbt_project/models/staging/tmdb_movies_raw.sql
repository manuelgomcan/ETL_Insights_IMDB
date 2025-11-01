{{
     config(
        materialized = 'incremental',
        unique_key = 'id',
        incremental_strategy = 'merge',
        on_schema_change = 'sync_all_columns'
    )
}}

with source_data as (
     select
        id,
        title,
        vote_average,
        vote_count,
        "status" as movie_status,
        release_date,
        revenue,
        runtime,
        budget,
        imdb_id,
        original_language,
        original_title,
        overview,
        popularity,
        tagline,
        genres,
        production_companies,
        production_countries,
        spoken_languages,
        "cast" as movie_cast,
        director,
        director_of_photography,
        writers,
        producers,
        music_composer,
        imdb_rating,
        imdb_votes,
        poster_path,
        _dlt_load_id,
        _dlt_id
    from {{ source('tmdb', 'imdb_raw') }}
    where id is not null
      and title is not null
      and release_date is not null
)

select * 
from source_data
{% if is_incremental() %}
where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
   or not exists (select 1 from {{ this }} where {{ this }}.id = source_data.id)
{% endif %}