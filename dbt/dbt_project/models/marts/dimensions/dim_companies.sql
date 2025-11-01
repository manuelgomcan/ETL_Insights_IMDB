{{
    config(
        materialized='incremental',
        unique_key='company_name',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        tags=['core', 'dimension'],
        meta={
            'surrogate_key': true
        }
    )
}}

with companies as (
    select distinct
        company_name,
        max(_dlt_load_id) as _dlt_load_id
    from {{ ref('stg_companies') }}
    where company_name != ''
    group by 1
)

select
    {{ dbt_utils.generate_surrogate_key(['company_name']) }} as company_id,
    company_name,
    _dlt_load_id
from companies
where company_name != ''
{% if is_incremental() %}
    and (_dlt_load_id > (select max(_dlt_load_id) from {{ this }})
        or not exists (select 1 from {{ this }} where {{ this }}.company_name = companies.company_name))
{% endif %}
