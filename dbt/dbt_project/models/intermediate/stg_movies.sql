-- depends_on: {{ ref('tmdb_movies_raw') }}


{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns',
        tags=['movies', 'core']
    )
}}

with raw as (
    select * from {{ ref('tmdb_movies_raw') }}
)

select
    id,
    trim(title) as title,
    case when vote_average = '' then null else cast(vote_average as numeric) end as vote_average,
    case when vote_count = '' then null else cast(floor(cast(vote_count as numeric)) as integer) end as vote_count,
    movie_status,  
    case 
        when id is not null
        and title is not null
        and release_date != '' 
        then cast(release_date as date) 
        else null 
    end as release_date,
    case when revenue = '' then null else cast(revenue as numeric) end as revenue,
    case when runtime = '' then null else cast(floor(cast(runtime as numeric)) as integer) end as runtime,
    case when budget = '' then null else cast(budget as numeric) end as budget,
    imdb_id,
    lower(original_language) as original_language,
    original_title,
    overview,
    case when popularity = '' then null else cast(popularity as numeric) end as popularity,
    tagline,
    genres,
    production_companies,
    production_countries,
    spoken_languages,
    case when imdb_rating = '' then null else cast(imdb_rating as numeric) end as imdb_rating,
    case when imdb_votes = '' then null else cast(floor(cast(imdb_votes as numeric)) as integer) end as imdb_votes,
    poster_path,
    _dlt_load_id
from raw
{% if is_incremental() %}
where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
   or id not in (select id from {{ this }})
{% endif %}
