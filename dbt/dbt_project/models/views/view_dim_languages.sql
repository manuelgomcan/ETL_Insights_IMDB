{{
    config(
        materialized='view',
        tags=['views', 'powerbi']
    )
}}

-- Vista para PowerBI de la dimensión de idiomas
select * from {{ ref('dim_languages') }}
