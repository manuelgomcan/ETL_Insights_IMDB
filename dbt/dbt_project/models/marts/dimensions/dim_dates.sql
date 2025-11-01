{{
    config(
        materialized = "table",
        tags=['dimensions', 'dimension'],
        meta={
            'surrogate_key': true
        }
    )
}}

with date_dimension as (
    {{ dbt_date.get_date_dimension("1900-01-01", "2050-12-31") }}
),

final as (
    select
        date_day as date_id,
        date_day,
        day_of_week,
        day_of_week_name,
        day_of_month,
        day_of_year,
        week_of_year,
        month_of_year,
        month_name,
        quarter_of_year,
        year_number,
        
        case 
            when month_of_year in (6,7,8) then 'Summer Season'
            when month_of_year in (11,12) then 'Holiday Season'
            else 'Regular Season'
        end as movie_season,
        
        case
            when day_of_week in (1,7) then true
            else false
        end as is_weekend,
        
        current_timestamp as dbt_loaded_at
    from date_dimension
)

select * from final
