import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


project_id = 'auspicious-lead-382321'
dataset_name = 'movie_raw_af'
bucket_name = 'movie_data_lpez'
region = 'us-central1'

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

dag = DAG(
    dag_id='ingest-controller',
    default_args=default_args,
    description='controller dag',
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)

# create dataset
create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset_if_needed',
    project_id=project_id,
    dataset_id=dataset_name,
    location=region,
    if_exists='ignore',
    dag=dag)

# #load amc_snack
amc_snack_table_name = 'amc_snack'
amc_snack_file_name = 'initial_load/amc_snack.csv'
amc_snack_delimiter = ','
amc_snack_skip_leading_rows = 1

amc_snack_schema_full = [
    {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "calories", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "calories_from_fat", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "total_fat", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "saturated_fat", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "trans_fat", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "polyunsat_fat", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "monounsat_fat", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "cholesterol", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "sodium", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "potassium", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "carbohydrates", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "dietary_fiber", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "sugars", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "protein", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "data_source", "type": "STRING", "mode": "REQUIRED"},    
    {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"}
]

amc_snack_schema = [
    {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "calories", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "calories_from_fat", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "total_fat", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "saturated_fat", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "trans_fat", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "polyunsat_fat", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "monounsat_fat", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "cholesterol", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "sodium", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "potassium", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "carbohydrates", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "dietary_fiber", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "sugars", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "protein", "type": "FLOAT", "mode": "NULLABLE"}
]

amc_snack = TriggerDagRunOperator(
    task_id="amc_snack",
    trigger_dag_id="ingest-table",
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": amc_snack_table_name, "file_name": amc_snack_file_name, \
           "delimiter": amc_snack_delimiter, "skip_leading_rows": amc_snack_skip_leading_rows, \
           "schema_full": amc_snack_schema_full, "schema": amc_snack_schema},
    dag=dag)

# #load github_theater_location
github_theater_location_table_name = 'github_theater_location'
github_theater_location_file_name = 'initial_load/github_theater_location.csv'
github_theater_location_delimiter = ','
github_theater_location_skip_leading_rows = 1

github_theater_location_schema_full = [
    {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "address", "type": "STRING", "mode": "REQUIRED"},
    {"name": "city", "type": "STRING", "mode": "REQUIRED"},
    {"name": "state", "type": "STRING", "mode": "REQUIRED"},
    {"name": "zip", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "country", "type": "STRING", "mode": "REQUIRED"},
    {"name": "theaterChainName", "type": "STRING", "mode": "REQUIRED"},
    {"name": "location", "type": "STRING", "mode": "REQUIRED"},
    {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"}
]

github_theater_location_schema = [
    {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "address", "type": "STRING", "mode": "REQUIRED"},
    {"name": "city", "type": "STRING", "mode": "REQUIRED"},
    {"name": "state", "type": "STRING", "mode": "REQUIRED"},
    {"name": "zip", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "country", "type": "STRING", "mode": "REQUIRED"},
    {"name": "theaterChainName", "type": "STRING", "mode": "REQUIRED"},
    {"name": "location", "type": "STRING", "mode": "REQUIRED"}
]

github_theater_location = TriggerDagRunOperator(
    task_id="github_theater_location",
    trigger_dag_id="ingest-table",
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": github_theater_location_table_name, "file_name": github_theater_location_file_name, \
           "delimiter": github_theater_location_delimiter, "skip_leading_rows": github_theater_location_skip_leading_rows, \
           "schema_full": github_theater_location_schema_full, "schema": github_theater_location_schema},
    dag=dag)

#load imdb_movie
imdb_movie_table_name = 'imdb_movie'
imdb_movie_file_name = 'initial_load/imdb_movie.csv'
imdb_movie_delimiter = ','
imdb_movie_skip_leading_rows = 1

imdb_movie_schema_full = [
    {"name": "tconst", "type": "STRING", "mode": "REQUIRED"},
    {"name": "title", "type": "STRING", "mode": "REQUIRED"},
    {"name": "release_year", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "runtime_minutes", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "genres", "type": "STRING", "mode": "NULLABLE"},
    {"name": "directors", "type": "STRING", "mode": "NULLABLE"},
    {"name": "writers", "type": "STRING", "mode": "NULLABLE"},
    {"name": "average_rating", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "num_ratings", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "actors", "type": "STRING", "mode": "NULLABLE"},
    {"name": "data_source", "type": "STRING", "mode": "REQUIRED"},        
    {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"}
]

imdb_movie_schema = [
    {"name": "tconst", "type": "STRING", "mode": "REQUIRED"},
    {"name": "title", "type": "STRING", "mode": "REQUIRED"},
    {"name": "release_year", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "runtime_minutes", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "genres", "type": "STRING", "mode": "NULLABLE"},
    {"name": "directors", "type": "STRING", "mode": "NULLABLE"},
    {"name": "writers", "type": "STRING", "mode": "NULLABLE"},
    {"name": "average_rating", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "num_ratings", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "actors", "type": "STRING", "mode": "NULLABLE"}
]

imdb_movie = TriggerDagRunOperator(
    task_id="imdb_movie",
    trigger_dag_id="ingest-table",
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": imdb_movie_table_name, "file_name": imdb_movie_file_name, \
           "delimiter": imdb_movie_delimiter, "skip_leading_rows": imdb_movie_skip_leading_rows, \
           "schema_full": imdb_movie_schema_full, "schema": imdb_movie_schema},
    dag=dag)

#load tmdb_actor
tmdb_actor_table_name = 'tmdb_actor'
tmdb_actor_file_name = 'initial_load/tmdb_actor.csv'
tmdb_actor_delimiter = ','
tmdb_actor_skip_leading_rows = 1

tmdb_actor_schema_full = [
    {"name": "credit_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "person_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "movie_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "character", "type": "STRING", "mode": "NULLABLE"},
    {"name": "gender", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "credit_order", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "data_source", "type": "STRING", "mode": "REQUIRED"},    
    {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"}
]

tmdb_actor_schema = [
    {"name": "credit_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "person_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "movie_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "character", "type": "STRING", "mode": "NULLABLE"},
    {"name": "gender", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "credit_order", "type": "INTEGER", "mode": "REQUIRED"}
]

tmdb_actor = TriggerDagRunOperator(
    task_id="tmdb_actor",
    trigger_dag_id="ingest-table",
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": tmdb_actor_table_name, "file_name": tmdb_actor_file_name, \
           "delimiter": tmdb_actor_delimiter, "skip_leading_rows": tmdb_actor_skip_leading_rows, \
           "schema_full": tmdb_actor_schema_full, "schema": tmdb_actor_schema},
    dag=dag)

#load tmdb_character
tmdb_character_table_name = 'tmdb_character'
tmdb_character_file_name = 'initial_load/tmdb_character.csv'
tmdb_character_delimiter = ','
tmdb_character_skip_leading_rows = 1

tmdb_character_schema_full = [
    {"name": "credit_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "person_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "movie_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "character", "type": "STRING", "mode": "REQUIRED"},
    {"name": "credit_order", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "data_source", "type": "STRING", "mode": "REQUIRED"},    
    {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"}
]

tmdb_character_schema = [
    {"name": "credit_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "person_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "movie_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "character", "type": "STRING", "mode": "REQUIRED"},
    {"name": "credit_order", "type": "INTEGER", "mode": "NULLABLE"}
]

tmdb_character = TriggerDagRunOperator(
    task_id="tmdb_character",
    trigger_dag_id="ingest-table",
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": tmdb_character_table_name, "file_name": tmdb_character_file_name, \
           "delimiter": tmdb_character_delimiter, "skip_leading_rows": tmdb_character_skip_leading_rows, \
           "schema_full": tmdb_character_schema_full, "schema": tmdb_character_schema},
    dag=dag)

# #load tmdb_crew
tmdb_crew_table_name = 'tmdb_crew'
tmdb_crew_file_name = 'initial_load/tmdb_crew.csv'
tmdb_crew_delimiter = ','
tmdb_crew_skip_leading_rows = 1

tmdb_crew_schema_full = [
    {"name": "credit_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "person_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "movie_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "gender", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "department", "type": "STRING", "mode": "NULLABLE"},
    {"name": "known_for_department", "type": "STRING", "mode": "NULLABLE"},
    {"name": "job", "type": "STRING", "mode": "REQUIRED"},
    {"name": "data_source", "type": "STRING", "mode": "REQUIRED"},        
    {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"}
]

tmdb_crew_schema = [
    {"name": "credit_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "person_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "movie_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "gender", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "department", "type": "STRING", "mode": "NULLABLE"},
    {"name": "known_for_department", "type": "STRING", "mode": "NULLABLE"},
    {"name": "job", "type": "STRING", "mode": "REQUIRED"},
]

tmdb_crew = TriggerDagRunOperator(
    task_id="tmdb_crew",
    trigger_dag_id="ingest-table",
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": tmdb_crew_table_name, "file_name": tmdb_crew_file_name, \
           "delimiter": tmdb_crew_delimiter, "skip_leading_rows": tmdb_crew_skip_leading_rows, \
           "schema_full": tmdb_crew_schema_full, "schema": tmdb_crew_schema},
    dag=dag)

# #load tmdb_external_ids
tmdb_external_ids_table_name = 'tmdb_external_ids'
tmdb_external_ids_file_name = 'initial_load/tmdb_external_ids.csv'
tmdb_external_ids_delimiter = ','
tmdb_external_ids_skip_leading_rows = 1

tmdb_external_ids_schema_full = [
    {"name": "tmdb_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "imdb_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "wikidata_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "facebook_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "instagram_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "twitter_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "data_source", "type": "STRING", "mode": "REQUIRED"},    
    {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"}
]

tmdb_external_ids_schema = [
    {"name": "tmdb_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "imdb_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "wikidata_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "facebook_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "instagram_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "twitter_id", "type": "STRING", "mode": "NULLABLE"}
]

tmdb_external_ids = TriggerDagRunOperator(
    task_id="tmdb_external_ids",
    trigger_dag_id="ingest-table",
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": tmdb_external_ids_table_name, "file_name": tmdb_external_ids_file_name, \
           "delimiter": tmdb_external_ids_delimiter, "skip_leading_rows": tmdb_external_ids_skip_leading_rows, \
           "schema_full": tmdb_external_ids_schema_full, "schema": tmdb_external_ids_schema},
    dag=dag)

# #load tmdb_genre
tmdb_genre_table_name = 'tmdb_genre'
tmdb_genre_file_name = 'initial_load/tmdb_genre.csv'
tmdb_genre_delimiter = ','
tmdb_genre_skip_leading_rows = 1

tmdb_genre_schema_full = [
    {"name": "genre_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "data_source", "type": "STRING", "mode": "REQUIRED"},    
    {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"}
]

tmdb_genre_schema = [
    {"name": "genre_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "REQUIRED"}
]

tmdb_genre = TriggerDagRunOperator(
    task_id="tmdb_genre",
    trigger_dag_id="ingest-table",
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": tmdb_genre_table_name, "file_name": tmdb_genre_file_name, \
           "delimiter": tmdb_genre_delimiter, "skip_leading_rows": tmdb_genre_skip_leading_rows, \
           "schema_full": tmdb_genre_schema_full, "schema": tmdb_genre_schema},
    dag=dag)

# #load tmdb_movie
tmdb_movie_table_name = 'tmdb_movie'
tmdb_movie_file_name = 'initial_load/tmdb_movie.csv'
tmdb_movie_delimiter = ','
tmdb_movie_skip_leading_rows = 1

tmdb_movie_schema_full = [
    {"name": "record_date", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "movie_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "title", "type": "STRING", "mode": "NULLABLE"},
    {"name": "original_language", "type": "STRING", "mode": "NULLABLE"},
    {"name": "overview", "type": "STRING", "mode": "NULLABLE"},
    {"name": "popularity", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "release_date", "type": "STRING", "mode": "NULLABLE"},
    {"name": "genre_ids", "type": "STRING", "mode": "NULLABLE"},
    {"name": "vote_average", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "vote_count", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"}
]

tmdb_movie_schema = [
    {"name": "record_date", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "movie_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "title", "type": "STRING", "mode": "NULLABLE"},
    {"name": "original_language", "type": "STRING", "mode": "NULLABLE"},
    {"name": "overview", "type": "STRING", "mode": "NULLABLE"},
    {"name": "popularity", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "release_date", "type": "STRING", "mode": "NULLABLE"},
    {"name": "genre_ids", "type": "STRING", "mode": "NULLABLE"},
    {"name": "vote_average", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "vote_count", "type": "INTEGER", "mode": "NULLABLE"}
]

tmdb_movie = TriggerDagRunOperator(
    task_id="tmdb_movie",
    trigger_dag_id="ingest-table",
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": tmdb_movie_table_name, "file_name": tmdb_movie_file_name, \
           "delimiter": tmdb_movie_delimiter, "skip_leading_rows": tmdb_movie_skip_leading_rows, \
           "schema_full": tmdb_movie_schema_full, "schema": tmdb_movie_schema},
    dag=dag)

# #load tmdb_movie_genre
tmdb_movie_genre_table_name = 'tmdb_movie_genre'
tmdb_movie_genre_file_name = 'initial_load/tmdb_movie_genre.csv'
tmdb_movie_genre_delimiter = ','
tmdb_movie_genre_skip_leading_rows = 1

tmdb_movie_genre_schema_full = [
    {"name": "movie_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "genre_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "data_source", "type": "STRING", "mode": "REQUIRED"},
    {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"}
]

tmdb_movie_genre_schema = [    
    {"name": "movie_genre_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "movie_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "genre_id", "type": "INTEGER", "mode": "REQUIRED"},
]

tmdb_movie_genre = TriggerDagRunOperator(
    task_id="tmdb_movie_genre",
    trigger_dag_id="ingest-table",
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": tmdb_movie_genre_table_name, "file_name": tmdb_movie_genre_file_name, \
           "delimiter": tmdb_movie_genre_delimiter, "skip_leading_rows": tmdb_movie_genre_skip_leading_rows, \
           "schema_full": tmdb_movie_genre_schema_full, "schema": tmdb_movie_genre_schema},
    dag=dag)

# #load tmdb_person_info
tmdb_p_i_table_name = 'tmdb_person_info'
tmdb_p_i_file_name = 'initial_load/tmdb_person_info.csv'
tmdb_p_i_delimiter = ','
tmdb_p_i_skip_leading_rows = 1

tmdb_p_i_schema_full = [
    {"name": "person_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "also_known_as", "type": "STRING", "mode": "NULLABLE"},
    {"name": "biography", "type": "STRING", "mode": "NULLABLE"},
    {"name": "birthday", "type": "STRING", "mode": "NULLABLE"},
    {"name": "deathday", "type": "STRING", "mode": "NULLABLE"},
    {"name": "gender", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "homepage", "type": "STRING", "mode": "NULLABLE"},
    {"name": "place_of_birth", "type": "STRING", "mode": "NULLABLE"},
    {"name": "imdb_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "known_for_department", "type": "STRING", "mode": "NULLABLE"},
    {"name": "popularity", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "load_time", "type": "TIMESTAMP", "mode": "REQUIRED", "default_value_expression": "CURRENT_TIMESTAMP"}
]

tmdb_p_i_schema = [
    {"name": "person_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "also_known_as", "type": "STRING", "mode": "NULLABLE"},
    {"name": "biography", "type": "STRING", "mode": "NULLABLE"},
    {"name": "birthday", "type": "STRING", "mode": "NULLABLE"},
    {"name": "deathday", "type": "STRING", "mode": "NULLABLE"},
    {"name": "gender", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "homepage", "type": "STRING", "mode": "NULLABLE"},
    {"name": "place_of_birth", "type": "STRING", "mode": "NULLABLE"},
    {"name": "imdb_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "known_for_department", "type": "STRING", "mode": "NULLABLE"},
    {"name": "popularity", "type": "FLOAT", "mode": "NULLABLE"},
]

tmdb_person_info = TriggerDagRunOperator(
    task_id="tmdb_person_info",
    trigger_dag_id="ingest-table",
    conf={"project_id": project_id, "dataset_name": dataset_name, "bucket_name": bucket_name, \
           "table_name": tmdb_p_i_table_name, "file_name": tmdb_p_i_file_name, \
           "delimiter": tmdb_p_i_delimiter, "skip_leading_rows": tmdb_p_i_skip_leading_rows, \
           "schema_full": tmdb_p_i_schema_full, "schema": tmdb_p_i_schema},
    dag=dag)

start >> create_dataset >> amc_snack >> end
start >> create_dataset >> github_theater_location >> end
start >> create_dataset >> imdb_movie >> end
start >> create_dataset >> tmdb_actor >> end
start >> create_dataset >> tmdb_character >> end
start >> create_dataset >> tmdb_crew >> end
start >> create_dataset >> tmdb_external_ids >> end
start >> create_dataset >> tmdb_genre >> end
start >> create_dataset >> tmdb_movie >> end
start >> create_dataset >> tmdb_movie_genre >> end
start >> create_dataset >> tmdb_person_info >> end