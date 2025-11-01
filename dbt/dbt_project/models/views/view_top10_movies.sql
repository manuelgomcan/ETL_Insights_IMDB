{{
    config(
        materialized='view',
        tags=['views', 'top10']
    )
}}


with movie_ratings as (
    select
        dm.movie_id,
        dm.title,
        dm.release_date,
        dm.poster_path,
        'https://image.tmdb.org/t/p/w500' || dm.poster_path as poster_url,
        fp.rating,
        fp.vote_count,
        fp.revenue,
        fp.budget,
        fp.popularity,
        extract(year from dm.release_date) as release_year
    from {{ ref('dim_movies') }} dm
    inner join {{ ref('fact_movie_performance') }} fp
        on dm.movie_id = fp.movie_id
    where dm.poster_path is not null
        and fp.rating > 0
        and fp.vote_count > 100  -- Filtrar películas con un mínimo de votos 
)

select * from (
    select
        movie_id,
        title,
        release_date,
        poster_path,
        poster_url,
        rating,
        vote_count,
        revenue,
        budget,
        popularity,
        release_year,
        row_number() over (partition by release_year order by rating desc, vote_count desc) as rank_by_year,
        row_number() over (order by rating desc, vote_count desc) as rank_overall
    from movie_ratings
) ranked_movies
where rank_overall <= 10  -- Solo incluir las 10 mejores películas en general
