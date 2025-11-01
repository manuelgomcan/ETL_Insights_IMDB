{{
    config(
        materialized='view',
        tags=['views', 'powerbi']
    )
}}

-- Vista para PowerBI de la tabla de hechos del reparto de películas
select * from {{ ref('fact_movie_cast') }}
