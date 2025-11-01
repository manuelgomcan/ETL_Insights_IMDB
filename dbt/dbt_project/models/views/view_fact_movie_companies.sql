{{
    config(
        materialized='view',
        tags=['views', 'powerbi']
    )
}}

-- Vista para PowerBI de la tabla de hechos de compañías de películas
select * from {{ ref('fact_movie_companies') }}
