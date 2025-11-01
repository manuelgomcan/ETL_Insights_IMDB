{{
    config(
        materialized='view',
        tags=['views', 'top10']
    )
}}


with genre_ratings as (
    select
        dg.genre_id,
        dg.genre_name,
        dm.release_date,
        extract(year from dm.release_date) as release_year,
        fp.rating,
        fp.vote_count,
        fp.revenue
    from {{ ref('dim_genres') }} dg
    inner join {{ ref('fact_movie_genres') }} fmg
        on dg.genre_id = fmg.genre_id
    inner join {{ ref('dim_movies') }} dm
        on fmg.movie_id = dm.movie_id
    inner join {{ ref('fact_movie_performance') }} fp
        on dm.movie_id = fp.movie_id
    where fp.rating > 0
        and fp.vote_count > 100  -- Filtrar películas con un mínimo de votos 

genre_avg_ratings as (
    select
        genre_id,
        genre_name,
        count(distinct release_year) as years_active,
        count(*) as num_movies,
        avg(rating) as avg_rating,
        sum(vote_count) as total_votes,
        sum(revenue) as total_revenue,
        min(release_date) as first_movie_date,
        max(release_date) as last_movie_date
    from genre_ratings
    group by genre_id, genre_name
    having count(*) >= 5  -- Géneros con al menos 5 películas
)

select * from (
    select
        genre_id,
        genre_name,
        years_active,
        num_movies,
        avg_rating,
        total_votes,
        total_revenue,
        first_movie_date,
        last_movie_date,
        row_number() over (order by avg_rating desc, total_votes desc) as rank_by_rating,
        row_number() over (order by total_revenue desc) as rank_by_revenue
    from genre_avg_ratings
) ranked_genres
where rank_by_rating <= 10  -- Solo incluir los 10 mejores géneros por rating
