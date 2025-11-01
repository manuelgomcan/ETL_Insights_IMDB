-- depends_on: {{ ref('stg_directors') }}
-- depends_on: {{ ref('directors_snapshot') }}
-- depends_on: {{ ref('movies_snapshot') }}

{{
    config(
        materialized='incremental',
        unique_key='movie_director_sk',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        tags=['facts', 'directors']
    )
}}

/*
    Tabla de hechos para directores de películas
    
    Siguiendo el enfoque SCD Type 2 del ejemplo de Blancanieves:
    - La dimensión tiene SK (surrogate key), BK (business key), y fechas From/To
    - La tabla de hechos se relaciona con la dimensión usando la SK
*/

with movie_dates as (
    -- Extraemos las fechas de las películas para determinar qué versión del director aplicaba
    select distinct
        movie_id,
        title,
        release_date,
        _dlt_load_id
    from {{ ref('movies_snapshot') }}
    where dbt_valid_to is null  -- Versión actual de la película
),

director_relationships as (
    -- Obtenemos las relaciones director-película de la capa de staging
    select distinct
        movie_id,
        person_name as director_name,
        _dlt_load_id
    from {{ ref('stg_directors') }}
    where person_name is not null and person_name != ''
),

director_versions as (
    -- Obtenemos todas las versiones de los directores del snapshot
    select distinct
        -- Esta es la SK (surrogate key) que identifica cada versión del director
        {{ dbt_utils.generate_surrogate_key(['director_name', 'dbt_valid_from']) }} as director_version_sk,
        -- Esta es la BK (business key) que identifica al director a través de todas sus versiones
        director_name,
        director_id,
        _dlt_load_id,
        -- Estas son las fechas From/To que indican cuándo era válida cada versión
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('directors_snapshot') }}
)

select distinct
    -- Generamos una clave surrogate para la tabla de hechos
    {{ dbt_utils.generate_surrogate_key(['dr.movie_id', 'dr.director_name', 'dv.dbt_valid_from']) }} as movie_director_sk,
    -- Incluimos la BK (business key) de la película
    dr.movie_id,
    -- Incluimos la SK (surrogate key) de la versión del director
    dv.director_version_sk,
    -- Incluimos la BK (business key) del director
    dv.director_name,
    -- Incluimos el ID del director generado en dim_directors
    dv.director_id,
    -- Incluimos información adicional de la película
    md.title as movie_title,
    md.release_date,
    -- Incluimos las fechas From/To de la versión del director
    dv.dbt_valid_from as director_valid_from,
    dv.dbt_valid_to as director_valid_to,
    -- Usamos _dlt_load_id para incrementalidad
    dr._dlt_load_id
from director_relationships dr
join director_versions dv
    on dv.director_name = dr.director_name
    -- Filtramos para obtener la versión actual del director
    and dv.dbt_valid_to is null
join movie_dates md
    on md.movie_id = dr.movie_id
{% if is_incremental() %}
where (dr._dlt_load_id > (select max(_dlt_load_id) from {{ this }})
       or not exists (
           select 1 from {{ this }}
           where movie_id = dr.movie_id and director_name = dr.director_name
       ))
{% endif %}
