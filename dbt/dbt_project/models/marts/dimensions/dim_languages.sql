{{
    config(
        materialized='incremental',
        unique_key='language_name',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        tags=['core', 'dimension'],
        meta={
            'surrogate_key': true
        }
    )
}}


with languages as (
    select distinct
        language_name,
        max(_dlt_load_id) as _dlt_load_id
    from {{ ref('stg_languages') }}
    where language_name != ''
    group by 1
)

select
    {{ dbt_utils.generate_surrogate_key(['language_name']) }} as language_id,
    language_name,
    _dlt_load_id
from languages
where language_name != ''
{% if is_incremental() %}
    and (_dlt_load_id > (select max(_dlt_load_id) from {{ this }})
        or not exists (select 1 from {{ this }} where {{ this }}.language_name = languages.language_name))
{% endif %}
