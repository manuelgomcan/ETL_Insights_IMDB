-- depends_on: {{ ref('stg_movies') }}
-- depends_on: {{ ref('movies_snapshot') }}

{{
    config(
        materialized='incremental',
        unique_key='movie_performance_sk',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        tags=['finance', 'fact']
    )
}}

/*
    Tabla de hechos para rendimiento de películas
    
    Siguiendo el enfoque SCD Type 2:
    - Usa snapshots para mantener el historial de cambios
    - Relaciona hechos con la versión correcta de la dimensión según fechas
*/

with base_metrics as (
    select distinct
        s.id as movie_id,
        s.revenue,
        s.budget,
        coalesce(s.imdb_rating, 0) as rating,
        coalesce(s.imdb_votes, 0) as vote_count,
        coalesce(s.popularity, 0) as popularity,
        s._dlt_load_id
    from {{ ref('stg_movies') }} s
    where s.revenue IS NOT NULL
    and s.revenue > 1 -- Filtrar películas con al menos 1 millón en revenue
),

movie_snapshot_versions as (
    select distinct
        -- Generamos una clave surrogate única para cada versión de la película
        {{ dbt_utils.generate_surrogate_key(['movie_id', 'dbt_valid_from']) }} as movie_version_sk,
        movie_id,
        title,
        release_date,
        extract(year from release_date) as release_year,
        extract(quarter from release_date) as release_quarter,
        _dlt_load_id,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('movies_snapshot') }}
)

select distinct
    {{ dbt_utils.generate_surrogate_key(['mm.movie_id', 'ms.dbt_valid_from']) }} as movie_performance_sk,
    mm.movie_id,
    ms.movie_version_sk,
    ms.title as movie_title,
    ms.release_year,
    ms.release_quarter,
    ms.release_date,
    mm.revenue,
    mm.budget,
    mm.rating,
    mm.vote_count,
    mm.popularity,
    ms.dbt_valid_from as movie_valid_from,
    ms.dbt_valid_to as movie_valid_to,
    -- Usamos _dlt_load_id para incrementalidad
    mm._dlt_load_id
from base_metrics mm
join movie_snapshot_versions ms
    on mm.movie_id = ms.movie_id
{% if is_incremental() %}
where mm._dlt_load_id > (select max(_dlt_load_id) from {{ this }})
      or not exists (
          select 1 from {{ this }}
          where movie_id = mm.movie_id
      )
{% endif %}
