{{
    config(
        materialized='view',
        tags=['views', 'powerbi']
    )
}}

-- Vista para PowerBI de la tabla de hechos de idiomas de películas
select * from {{ ref('fact_movie_languages') }}
