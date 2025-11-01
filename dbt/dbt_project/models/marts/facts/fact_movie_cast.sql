-- depends_on: {{ ref('stg_cast') }}
-- depends_on: {{ ref('actors_snapshot') }}
-- depends_on: {{ ref('movies_snapshot') }}

{{
    config(
        materialized='incremental',
        unique_key='movie_cast_sk',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        tags=['facts', 'cast']
    )
}}

/*
    Tabla de hechos para el reparto de películas
    
    Siguiendo el enfoque SCD Type 2:
    - Usa snapshots para mantener el historial de cambios
    - Relaciona hechos con la versión correcta de la dimensión según fechas
*/

with actor_snapshot_versions as (
    select distinct
        -- Generamos una clave surrogate única para cada versión del actor
        {{ dbt_utils.generate_surrogate_key(['actor_name', 'movie_id', 'dbt_valid_from']) }} as actor_version_sk,
        actor_name,
        movie_id,
        _dlt_load_id,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('actors_snapshot') }}
),

movie_snapshot_versions as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['movie_id', 'dbt_valid_from']) }} as movie_version_sk,
        movie_id,
        title,
        release_date,
        _dlt_load_id,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('movies_snapshot') }}
)

select distinct
    {{ dbt_utils.generate_surrogate_key(['c.movie_id', 'c.person_name', 'a.dbt_valid_from', 'm.dbt_valid_from']) }} as movie_cast_sk,
    c.movie_id,
    c.person_name as actor_name,
    a.actor_version_sk,
    m.movie_version_sk,
    m.title as movie_title,
    m.release_date,
    c.role_type,
    a.dbt_valid_from as actor_valid_from,
    a.dbt_valid_to as actor_valid_to,
    m.dbt_valid_from as movie_valid_from,
    m.dbt_valid_to as movie_valid_to,
    c._dlt_load_id
from {{ ref('stg_cast') }} c
join actor_snapshot_versions a
    on a.actor_name = c.person_name
    and a.movie_id = c.movie_id
join movie_snapshot_versions m
    on m.movie_id = c.movie_id
where c.person_name is not null and c.person_name != ''
{% if is_incremental() %}
    and (c._dlt_load_id > (select max(_dlt_load_id) from {{ this }})
         or not exists (
             select 1 from {{ this }}
             where movie_id = c.movie_id and actor_name = c.person_name
         ))
{% endif %}
