{{
    config(
        materialized='view',
        tags=['views', 'powerbi']
    )
}}

-- Vista para PowerBI de la dimensión de actores
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
ORDER BY actor_name
