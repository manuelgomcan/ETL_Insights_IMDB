{{
    config(
        materialized='view',
        tags=['views', 'powerbi']
    )
}}

-- Vista para PowerBI de la dimensión de géneros
select * from {{ ref('dim_genres') }}
