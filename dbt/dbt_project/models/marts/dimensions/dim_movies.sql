-- depends_on: {{ ref('stg_movies') }}
{{ 
  config(
        materialized='incremental',
        unique_key='movie_sk',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        tags=['core', 'dimension'],
        meta={
            'surrogate_key': true
        }
  )
}}

select
    {{ dbt_utils.generate_surrogate_key(['id']) }} as movie_sk,
    id as movie_id,
    trim(title) as title,
    movie_status,
    cast(release_date as date) as release_date,
    cast(runtime as integer) as runtime,
    imdb_id,
    lower(original_language) as original_language,
    original_title,
    overview,
    tagline,
    production_companies,
    production_countries,
    spoken_languages,
    poster_path,
    _dlt_load_id
from {{ ref('stg_movies') }}
{% if is_incremental() %}
where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
   or not exists (select 1 from {{ this }} where {{ this }}.movie_id = id)
{% endif %}
