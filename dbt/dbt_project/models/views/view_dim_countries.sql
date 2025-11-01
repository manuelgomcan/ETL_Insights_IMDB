{{
    config(
        materialized='view',
        tags=['views', 'powerbi']
    )
}}

-- Vista para PowerBI de la dimensión de países
select * from {{ ref('dim_countries') }}
