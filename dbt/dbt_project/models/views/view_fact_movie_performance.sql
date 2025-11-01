{{
    config(
        materialized='view',
        tags=['views', 'powerbi']
    )
}}

-- Vista para PowerBI de la tabla de hechos de rendimiento de películas
select * from {{ ref('fact_movie_performance') }}
