{{
    config(
        materialized='view',
        tags=['views', 'powerbi']
    )
}}

-- Vista para PowerBI de la dimensión de películas
select * from {{ ref('dim_movies') }}
