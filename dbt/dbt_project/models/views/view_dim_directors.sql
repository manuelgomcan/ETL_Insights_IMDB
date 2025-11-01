{{
    config(
        materialized='view',
        tags=['views', 'powerbi']
    )
}}

-- Vista para PowerBI de la dimensión de directores
WITH directors_data AS (
    SELECT DISTINCT
        person_name as director_name,
        count(distinct movie_id) as num_movies,
        max(_dlt_load_id) as _dlt_load_id
    FROM {{ ref('stg_directors') }}
    WHERE person_name != ''
    GROUP BY person_name
)

SELECT DISTINCT
    director_name,
    {{ dbt_utils.generate_surrogate_key(['director_name']) }} as director_id,
    num_movies,
    _dlt_load_id
FROM directors_data
ORDER BY director_name
