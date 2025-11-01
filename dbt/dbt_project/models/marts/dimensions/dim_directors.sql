{{
    config(
        materialized='incremental',
        unique_key='director_name',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        tags=['dimensions', 'dimension'],
        meta={
            'surrogate_key': true
        }
    )
}}

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
{% if is_incremental() %}
WHERE _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
   OR not exists (select 1 from {{ this }} where {{ this }}.director_name = directors_data.director_name)
{% endif %}
ORDER BY director_name
