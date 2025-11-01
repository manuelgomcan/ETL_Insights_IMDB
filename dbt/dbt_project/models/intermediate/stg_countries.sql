{{
    config(
        materialized='incremental',
        unique_key='country_name',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        tags=['intermediate', 'countries']
    )
}}

/*
    Tabla staging para países de producción
*/

with countries_base as (
    select distinct
        id as movie_id,
        trim(unnest(string_to_array(production_countries, ','))) as country_name,
        _dlt_load_id
    from {{ ref('tmdb_movies_raw') }}
    where production_countries is not null
        and production_countries != ''
)

select
    movie_id,
    country_name,
    _dlt_load_id
from countries_base
{% if is_incremental() %}
    where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
        or not exists (select 1 from {{ this }} where {{ this }}.country_name = countries_base.country_name)
{% endif %}
