import dlt
import csv
from datetime import datetime

# Resource para cargar datos en tabla temporal
@dlt.resource(
    name="imdb_pre",
    write_disposition="replace",  
    primary_key="id"
)
def load_movies_pre():
    with open("../data/TMDB_all_movies.csv", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            yield row

# Source para la tabla temporal
@dlt.source
def tmdb_source_pre():
    yield load_movies_pre()

def do_incremental():
    pipeline = dlt.pipeline(destination="postgres", dataset_name="staging")
    print("Merge in progress...")
    with pipeline.sql_client() as client:
        # Verificar si la tabla destino existe y si no la creamos 
        client.execute_sql(
            """
            CREATE TABLE IF NOT EXISTS staging.imdb_raw AS 
            SELECT * FROM staging.imdb_pre
            WHERE 1=0;
            """
        )
        print("Tabla destino verificada/creada")
        
        # Luego realizamos el MERGE
        client.execute_sql(
            """MERGE INTO staging.imdb_raw AS tgt
            USING staging.imdb_pre AS src
            ON (tgt.id=src.id)
            WHEN matched and tgt._dlt_load_id <> src._dlt_load_id
                THEN UPDATE set title=src.title, vote_average=src.vote_average, vote_count=src.vote_count, status=src.status, release_date=src.release_date, revenue=src.revenue, runtime=src.runtime,  budget=src.budget,  imdb_id=src.imdb_id,  original_language=src.original_language,  original_title=src.original_title,  overview=src.overview,  popularity=src.popularity,  genres=src.genres,  production_companies=src.production_companies,  production_countries=src.production_countries,  spoken_languages=src.spoken_languages,  "cast"=src."cast",  director=src.director,  director_of_photography=src.director_of_photography,  writers=src.writers,  producers=src.producers,  imdb_rating=src.imdb_rating,  imdb_votes=src.imdb_votes,  poster_path=src.poster_path,  "_dlt_load_id"=src."_dlt_load_id",  tagline=src.tagline,  music_composer=src.music_composer
            WHEN NOT MATCHED
                THEN INSERT (id, title, vote_average, vote_count, status, release_date, revenue, runtime, budget, imdb_id, original_language, original_title, overview, popularity, genres, production_companies, production_countries, spoken_languages, "cast", director, director_of_photography, writers, producers, imdb_rating, imdb_votes, poster_path, "_dlt_load_id", "_dlt_id", tagline, music_composer)
                VALUES (src.id, src.title, src.vote_average, src.vote_count, src.status, src.release_date, src.revenue, src.runtime, src.budget, src.imdb_id, src.original_language, src.original_title, src.overview, src.popularity, src.genres, src.production_companies, src.production_countries, src.spoken_languages, src."cast", src.director, src.director_of_photography, src.writers, src.producers, src.imdb_rating, src.imdb_votes, src.poster_path, src."_dlt_load_id", src."_dlt_id", src.tagline, src.music_composer);
            """)
    print("Merge done")
def main():
    # 1: Cargar en la tabla temporal imdb_pre
    pipeline_pre = dlt.pipeline(
        pipeline_name="imdb_pre_pipeline",
        destination="postgres",
        dataset_name="staging",
        progress="log"
    )
    load_info = pipeline_pre.run(tmdb_source_pre())
    print(f"Pre-load completado: {load_info}")

    # 2: Hacer merge 
    do_incremental()

if __name__ == "__main__":
    main()