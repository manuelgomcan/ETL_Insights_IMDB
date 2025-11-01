-- depends_on: {{ ref('stg_movies') }}
-- depends_on: {{ ref('genres_snapshot') }}
-- depends_on: {{ ref('movies_snapshot') }}

{{
    config(
        materialized='incremental',
        unique_key='movie_genre_sk',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        tags=['facts', 'bridge']
    )
}}

/*
    Tabla de hechos para géneros de películas
    
    Siguiendo el enfoque SCD Type 2 del ejemplo de Blancanieves:
    - La dimensión tiene SK (surrogate key), BK (business key), y fechas From/To
    - La tabla de hechos se relaciona con la dimensión usando la SK
*/

with movie_dates as (
    -- Extraemos las fechas de las películas para determinar qué versión del género aplicaba
    select distinct
        movie_id,
        title,
        release_date,
        _dlt_load_id
    from {{ ref('movies_snapshot') }}
    where dbt_valid_to is null  -- Versión actual de la película
),

movie_genre_relationships as (
    -- Obtenemos las relaciones género-película de la capa de staging
    select distinct
        id as movie_id,
        trim(unnest(string_to_array(genres, ','))) as genre_name,
        _dlt_load_id
    from {{ ref('stg_movies') }}
    where genres is not null
        and genres != ''
),

genre_counts as (
    -- Calculamos cuántos géneros tiene cada película
    select
        movie_id,
        count(*) as num_genres
    from movie_genre_relationships
    group by movie_id
),

genre_versions as (
    -- Obtenemos todas las versiones de los géneros del snapshot
    select distinct
        -- Esta es la SK (surrogate key) que identifica cada versión del género
        {{ dbt_utils.generate_surrogate_key(['genre_name', 'dbt_valid_from']) }} as genre_version_sk,
        -- Esta es la BK (business key) que identifica al género a través de todas sus versiones
        genre_name,
        genre_id,
        _dlt_load_id,
        -- Estas son las fechas From/To que indican cuándo era válida cada versión
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('genres_snapshot') }}
)

select distinct
    -- Generamos una clave surrogate para la tabla de hechos
    {{ dbt_utils.generate_surrogate_key(['mgr.movie_id', 'mgr.genre_name', 'gv.dbt_valid_from']) }} as movie_genre_sk,
    -- Incluimos la BK (business key) de la película
    mgr.movie_id,
    -- Incluimos la SK (surrogate key) de la versión del género
    gv.genre_version_sk,
    -- Incluimos la BK (business key) del género
    gv.genre_name,
    -- Incluimos el ID del género generado en dim_genres
    gv.genre_id,
    -- Incluimos el número de géneros de la película
    gc.num_genres,
    -- Incluimos información adicional de la película
    md.title as movie_title,
    md.release_date,
    -- Incluimos las fechas From/To de la versión del género
    gv.dbt_valid_from as genre_valid_from,
    gv.dbt_valid_to as genre_valid_to,
    -- Usamos _dlt_load_id para incrementalidad
    mgr._dlt_load_id
from movie_genre_relationships mgr
join genre_versions gv
    on gv.genre_name = mgr.genre_name
    -- Filtramos para obtener la versión actual del género
    and gv.dbt_valid_to is null
join genre_counts gc 
    on gc.movie_id = mgr.movie_id
join movie_dates md
    on md.movie_id = mgr.movie_id
where mgr.genre_name is not null and mgr.genre_name != ''
{% if is_incremental() %}
    and (mgr._dlt_load_id > (select max(_dlt_load_id) from {{ this }})
         or not exists (
             select 1 from {{ this }}
             where movie_id = mgr.movie_id and genre_name = mgr.genre_name
         ))
{% endif %}
