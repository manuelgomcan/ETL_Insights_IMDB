{{
    config(
        materialized='incremental',
        unique_key='company_name',
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns',
        tags=['intermediate', 'companies']
    )
}}

/*
    Tabla staging para compañías productoras
*/

with companies_base as (
    select distinct
        id as movie_id,
        trim(unnest(string_to_array(production_companies, ','))) as company_name,
        _dlt_load_id
    from {{ ref('tmdb_movies_raw') }}
    where production_companies is not null
        and production_companies != ''
)

select
    movie_id,
    company_name,
    _dlt_load_id
from companies_base
{% if is_incremental() %}
    where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
        or company_name not in (select company_name from {{ this }})
{% endif %}
