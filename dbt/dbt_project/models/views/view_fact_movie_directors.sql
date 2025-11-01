{{
    config(
        materialized='view',
        tags=['views', 'powerbi']
    )
}}

-- Vista para PowerBI de la tabla de hechos de directores de películas
select * from {{ ref('fact_movie_directors') }}
