-- depends_on: {{ ref('stg_movies') }}
-- depends_on: {{ ref('countries_snapshot') }}
-- depends_on: {{ ref('movies_snapshot') }}

{{
    config(
        materialized='incremental',
        unique_key='movie_country_sk',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        tags=['facts', 'bridge']
    )
}}

/*
    Tabla de hechos para países de producción de películas
    
    Siguiendo el enfoque SCD Type 2 del ejemplo de Blancanieves:
    - La dimensión tiene SK (surrogate key), BK (business key), y fechas From/To
    - La tabla de hechos se relaciona con la dimensión usando la SK
    - Ahora usamos countries_snapshot para implementar completamente SCD Type 2
*/

with movie_dates as (
    -- Extraemos las fechas de las películas para determinar qué versión aplicaba
    select distinct
        -- Esta es la SK (surrogate key) que identifica cada versión de la película
        {{ dbt_utils.generate_surrogate_key(['movie_id', 'dbt_valid_from']) }} as movie_version_sk,
        -- Esta es la BK (business key) que identifica a la película a través de todas sus versiones
        movie_id,
        title,
        release_date,
        _dlt_load_id,
        -- Estas son las fechas From/To que indican cuándo era válida cada versión
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('movies_snapshot') }}
    where dbt_valid_to is null  -- Versión actual de la película
),

country_versions as (
    -- Extraemos las versiones de los países del snapshot
    select distinct
        -- Esta es la SK (surrogate key) que identifica cada versión del país
        {{ dbt_utils.generate_surrogate_key(['country_name', 'dbt_valid_from']) }} as country_version_sk,
        -- Esta es la BK (business key) que identifica al país a través de todas sus versiones
        country_name,
        country_id,
        _dlt_load_id,
        -- Estas son las fechas From/To que indican cuándo era válida cada versión
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('countries_snapshot') }}
    where dbt_valid_to is null  -- Versión actual del país
),

movie_country_relationships as (
    -- Obtenemos las relaciones país-película de la capa de staging
    select distinct
        id as movie_id,
        trim(unnest(string_to_array(production_countries, ','))) as country_name,
        _dlt_load_id
    from {{ ref('stg_movies') }}
    where production_countries is not null
        and production_countries != ''
)

select distinct
    -- Generamos una clave surrogate para la tabla de hechos
    {{ dbt_utils.generate_surrogate_key(['mcr.movie_id', 'mcr.country_name', 'md.dbt_valid_from', 'cv.dbt_valid_from']) }} as movie_country_sk,
    -- Incluimos la BK (business key) de la película
    mcr.movie_id,
    -- Incluimos la SK (surrogate key) de la versión de la película
    md.movie_version_sk,
    -- Incluimos la SK (surrogate key) de la versión del país
    cv.country_version_sk,
    -- Incluimos el ID y nombre del país
    cv.country_id,
    cv.country_name,
    -- Incluimos información adicional de la película
    md.title as movie_title,
    md.release_date,
    -- Incluimos las fechas From/To de las versiones
    md.dbt_valid_from as movie_valid_from,
    md.dbt_valid_to as movie_valid_to,
    cv.dbt_valid_from as country_valid_from,
    cv.dbt_valid_to as country_valid_to,
    -- Usamos _dlt_load_id para incrementalidad
    mcr._dlt_load_id
from movie_country_relationships mcr
join country_versions cv
    on cv.country_name = mcr.country_name
join movie_dates md
    on md.movie_id = mcr.movie_id
where mcr.country_name is not null and mcr.country_name != ''
{% if is_incremental() %}
    and (mcr._dlt_load_id > (select max(_dlt_load_id) from {{ this }})
         or not exists (
             select 1 from {{ this }}
             where movie_id = mcr.movie_id and country_name = mcr.country_name
         ))
{% endif %}
