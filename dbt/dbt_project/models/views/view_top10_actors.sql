{{
    config(
        materialized='view',
        tags=['views', 'top10']
    )
}}


with actor_ratings as (
    select
        fmc.actor_name,
        da.actor_id,
        dm.release_date,
        extract(year from dm.release_date) as release_year,
        fp.rating,
        fp.vote_count
    from {{ ref('fact_movie_cast') }} fmc
    inner join {{ ref('dim_actors') }} da
        on fmc.actor_name = da.actor_name
    inner join {{ ref('dim_movies') }} dm
        on fmc.movie_id = dm.movie_id
    inner join {{ ref('fact_movie_performance') }} fp
        on dm.movie_id = fp.movie_id
    where fp.rating > 0
        and fp.vote_count > 100  -- Filtrar películas con un mínimo de votos para relevancia
),

actor_avg_ratings as (
    select
        actor_id,
        actor_name,
        count(distinct release_year) as years_active,
        count(*) as num_movies,
        avg(rating) as avg_rating,
        sum(vote_count) as total_votes,
        min(release_date) as first_movie_date,
        max(release_date) as last_movie_date
    from actor_ratings
    group by actor_id, actor_name
    having count(*) >= 3  -- Actores con al menos 3 películas
)

select * from (
    select
        actor_id,
        actor_name,
        years_active,
        num_movies,
        avg_rating,
        total_votes,
        first_movie_date,
        last_movie_date,
        row_number() over (order by avg_rating desc, total_votes desc) as rank_overall
    from actor_avg_ratings
) ranked_actors
where rank_overall <= 10  -- Solo incluir los 10 mejores actores
