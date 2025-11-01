-- depends_on: {{ ref('stg_movies') }}
-- depends_on: {{ ref('languages_snapshot') }}
-- depends_on: {{ ref('movies_snapshot') }}

{{
    config(
        materialized='incremental',
        unique_key='movie_language_sk',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        tags=['facts', 'bridge']
    )
}}

/*
    Tabla de hechos para idiomas de películas
    
    Siguiendo el enfoque SCD Type 2 del ejemplo de Blancanieves:
    - La dimensión tiene SK (surrogate key), BK (business key), y fechas From/To
    - La tabla de hechos se relaciona con la dimensión usando la SK
    - Ahora usamos languages_snapshot para implementar completamente SCD Type 2
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

language_versions as (
    -- Extraemos las versiones de los idiomas del snapshot
    select distinct
        -- Esta es la SK (surrogate key) que identifica cada versión del idioma
        {{ dbt_utils.generate_surrogate_key(['language_name', 'dbt_valid_from']) }} as language_version_sk,
        -- Esta es la BK (business key) que identifica al idioma a través de todas sus versiones
        language_name,
        language_id,
        _dlt_load_id,
        -- Estas son las fechas From/To que indican cuándo era válida cada versión
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('languages_snapshot') }}
    where dbt_valid_to is null  -- Versión actual del idioma
),

movie_language_relationships as (
    -- Obtenemos las relaciones idioma-película de la capa de staging
    select distinct
        id as movie_id,
        trim(unnest(string_to_array(spoken_languages, ','))) as language_name,
        _dlt_load_id
    from {{ ref('stg_movies') }}
    where spoken_languages is not null
        and spoken_languages != ''
)

select distinct
    -- Generamos una clave surrogate para la tabla de hechos
    {{ dbt_utils.generate_surrogate_key(['mlr.movie_id', 'mlr.language_name', 'md.dbt_valid_from', 'lv.dbt_valid_from']) }} as movie_language_sk,
    -- Incluimos la BK (business key) de la película
    mlr.movie_id,
    -- Incluimos la SK (surrogate key) de la versión de la película
    md.movie_version_sk,
    -- Incluimos la SK (surrogate key) de la versión del idioma
    lv.language_version_sk,
    -- Incluimos el ID y nombre del idioma
    lv.language_id,
    lv.language_name,
    -- Incluimos información adicional de la película
    md.title as movie_title,
    md.release_date,
    -- Incluimos las fechas From/To de las versiones
    md.dbt_valid_from as movie_valid_from,
    md.dbt_valid_to as movie_valid_to,
    lv.dbt_valid_from as language_valid_from,
    lv.dbt_valid_to as language_valid_to,
    -- Usamos _dlt_load_id para incrementalidad
    mlr._dlt_load_id
from movie_language_relationships mlr
join language_versions lv
    on lv.language_name = mlr.language_name
join movie_dates md
    on md.movie_id = mlr.movie_id
where mlr.language_name is not null and mlr.language_name != ''
{% if is_incremental() %}
    and (mlr._dlt_load_id > (select max(_dlt_load_id) from {{ this }})
         or not exists (
             select 1 from {{ this }}
             where movie_id = mlr.movie_id and language_name = mlr.language_name
         ))
{% endif %}
