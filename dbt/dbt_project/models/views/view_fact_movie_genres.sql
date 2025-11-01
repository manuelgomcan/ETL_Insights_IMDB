{{
    config(
        materialized='view',
        tags=['views', 'powerbi']
    )
}}

-- Vista para PowerBI de la tabla de hechos de géneros de películas
select * from {{ ref('fact_movie_genres') }}
