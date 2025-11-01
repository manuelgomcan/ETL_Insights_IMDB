{{
    config(
        materialized='incremental',
        unique_key='actor_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        tags=['dimensions', 'dimension'],
        meta={
            'surrogate_key': true
        }
    )
}}


WITH actor_movies AS (
    SELECT DISTINCT
        person_name,
        count(distinct movie_id) as num_movies,
        max(_dlt_load_id) as _dlt_load_id
    FROM {{ ref('stg_cast') }}
    WHERE person_name != ''
    GROUP BY person_name
)

SELECT DISTINCT
    person_name as actor_name,
    {{ dbt_utils.generate_surrogate_key(['person_name']) }} as actor_id,
    num_movies,
    _dlt_load_id
FROM actor_movies
{% if is_incremental() %}
having max(_dlt_load_id) > (select max(_dlt_load_id) from {{ this }})
   or not exists (select 1 from {{ this }} where {{ this }}.actor_name = person_name)
{% endif %}
order by person_name
