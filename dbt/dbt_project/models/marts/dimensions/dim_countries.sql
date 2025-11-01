{{
    config(
        materialized='incremental',
        unique_key='country_name',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        tags=['core', 'dimension'],
        meta={
            'surrogate_key': true
        }
    )
}}


with countries as (
    select distinct
        country_name,
        max(_dlt_load_id) as _dlt_load_id
    from {{ ref('stg_countries') }}
    where country_name != ''
    group by 1
)

select
    {{ dbt_utils.generate_surrogate_key(['country_name']) }} as country_id,
    country_name,
    _dlt_load_id
from countries
where country_name != ''
{% if is_incremental() %}
    and (_dlt_load_id > (select max(_dlt_load_id) from {{ this }})
        or not exists (select 1 from {{ this }} where {{ this }}.country_name = countries.country_name))
{% endif %}
