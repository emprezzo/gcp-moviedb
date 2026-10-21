import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.dates import days_ago
from datetime import timedelta
from pendulum import duration

project_id = 'auspicious-lead-382321'
raw_dataset_name = 'movie_raw_af'
stg_dataset_name = 'movie_stg_af'
region = 'us-central1'

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}
dag = DAG(
    dag_id='model-controller',
    default_args=default_args,
    description='controller dag',
    schedule_interval=None,
    max_active_runs=1,
    concurrency=3,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)


start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
join = DummyOperator(task_id="join", dag=dag)
wait = TimeDeltaSensor(task_id="wait", delta=duration(seconds=5), dag=dag)

# create stg dataset
create_dataset_stg = BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset_stg',
    project_id=project_id,
    dataset_id=stg_dataset_name,
    location=region,
    if_exists='ignore',
    dag=dag)

# Theater
# Note: don't propagate default current_timestamp() on the load_time column, 
# this rule should only be applied to the raw tables
theater_sql = (
f"""create or replace table movie_stg_af.Theater(
    id INT64 not null,
    name STRING not null,
    address STRING not null,
    city STRING not null,
    state STRING not null,
    zip INT64 not null,
    country STRING not null,
    theaterChainName STRING not null,
    longitude STRING not null,
    latitude STRING not null,
    data_source STRING not null,
    load_time TIMESTAMP not null)
    as
        select id, name, address, city, state, zip, country, theaterChainName, location_array[0] as longitude, location_array[1] as latitude, 'github' as data_source, load_time
        from
            (select *, split(location, ', ') as location_array
            from movie_raw_af.github_theater_location)""")

create_theater_stg = BigQueryInsertJobOperator(
    task_id="create_theater_stg",
    configuration={
        "query": {
            "query": theater_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


#Snacks
snack_sql = (
f"""create or replace table {stg_dataset_name}.Snack(
    id INT64 not null,
    name STRING not null,
    calories FLOAT64,
    calories_from_fat FLOAT64,
    total_fat FLOAT64,
    satured_fat FLOAT64,
    trans_fat FLOAT64,
    polyunsat_fat FLOAT64,
    monounsat_fat FLOAT64,
    cholesterol FLOAT64,
    sodium FLOAT64,
    potassium FLOAT64,
    carbohydrates FLOAT64,
    dietary_fiber FLOAT64,
    sugars FLOAT64,
    protein FLOAT64,
    data_source STRING not null,
    load_time TIMESTAMP not null)
    as
        select distinct * except(load_time), 'amc' as data_source, load_time
        from {raw_dataset_name}.amc_snack""")

create_snack_stg = BigQueryInsertJobOperator(
    task_id="create_snack_stg",
    configuration={
        "query": {
            "query": snack_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

#Actor
actor_sql = (
 f"""create or replace table {stg_dataset_name}.Actor(
    credit_id STRING not null,
    person_id INT64 not null,
    movie_id INT64 not null,
    name STRING not null,
    character STRING,
    gender INT64,
    credit_order INT64,
    data_source STRING not null,
    load_time TIMESTAMP not null)
    as
        select distinct * except(load_time), 'tmdb' as data_source, load_time
        from {raw_dataset_name}.tmdb_actor""")

create_actor_stg = BigQueryInsertJobOperator(
    task_id="create_actor_stg",
    configuration={
        "query": {
            "query": actor_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

#Character
character_sql = (
f"""create or replace table {stg_dataset_name}.Character(
    credit_id STRING not null,
    person_id INT64 not null,
    movie_id INT64 not null,
    name STRING not null,
    character STRING not null,
    credit_order INT64 not null,
    data_source STRING not null,
    load_time TIMESTAMP not null)
    as
        select distinct * except(load_time), 'tmdb' as data_source, load_time
        from {raw_dataset_name}.tmdb_character""")

create_character_stg = BigQueryInsertJobOperator(
    task_id="create_character_stg",
    configuration={
        "query": {
            "query": character_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

#Crew
crew_sql = (
f"""create or replace table {stg_dataset_name}.Crew(
    credit_id STRING not null,
    person_id INT64 not null,
    movie_id INT64 not null,
    name STRING not null,
    gender INT64 not null,
    department STRING,
    known_for_department STRING,
    job STRING,
    data_source STRING not null,
    load_time TIMESTAMP not null)
    as
        select distinct * except(load_time), 'tmdb' as data_source, load_time
        from {raw_dataset_name}.tmdb_crew""")

create_crew_stg = BigQueryInsertJobOperator(
    task_id="create_crew_stg",
    configuration={
        "query": {
            "query": crew_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

#Genre
genre_sql = (
f"""create or replace table {stg_dataset_name}.Genre(
    genre_id INT64 not null,
    name STRING not null,
    data_source STRING not null,
    load_time TIMESTAMP not null)
    as
        select distinct * except(load_time), 'tmdb' as data_source, load_time
        from {raw_dataset_name}.tmdb_genre""")

create_genre_stg = BigQueryInsertJobOperator(
    task_id="create_genre_stg",
    configuration={
        "query": {
            "query": genre_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

#Movie_Genre
movie_genre_sql = (
f"""create or replace table {stg_dataset_name}.Movie_Genre(
    movie_genre_id STRING not null,
    movie_id INT64 not null,
    genre_id INT64 not null,
    data_source STRING not null,
    load_time TIMESTAMP not null)
    
    AS
        select distinct concat(movie_id, '-', genre_id) as movie_genre_id, movie_id, genre_id, 'tmdb' as data_source, load_time
        from {raw_dataset_name}.tmdb_movie_genre""")

create_movie_genre_stg = BigQueryInsertJobOperator(
    task_id="create_movie_genre_stg",
    configuration={
        "query": {
            "query": movie_genre_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


#tmdb_movie (intermediate)
tmdb_movie_sql = (
f"""CREATE OR REPLACE TABLE {stg_dataset_name}.tmdb_movie(
  record_date TIMESTAMP not null,
  movie_id INT64 not null,
  title STRING,
  original_language STRING,
  overview STRING,
  popularity FLOAT64,
  release_date DATE,
  genre_ids STRING not null,
  vote_average FLOAT64,
  vote_count INT64,
  data_source STRING not null,
  load_time TIMESTAMP not null,
) 
as
    (select record_date, movie_id, title, original_language, REGEXP_EXTRACT(overview, r'^Title: .* \(\d{4}\)\. (.*)') as overview,
    popularity, PARSE_DATE('%m/%d/%Y', release_date) AS release_date, genre_ids, vote_average, vote_count, 'tmdb' as data_source, load_time
    from {raw_dataset_name}.tmdb_movie)""")

create_tmdb_movie_stg = BigQueryInsertJobOperator(
    task_id="create_tmdb_movie_stg",
    configuration={
        "query": {
            "query": tmdb_movie_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

#Tmdb_Movie (intermediate to be joined with raw imdb_movie)
Tmdb_Movie_sql = (
f"""CREATE OR REPLACE TABLE {stg_dataset_name}.Tmdb_Movie(
  movie_id INT64 not null,
  title STRING,
  original_language STRING,
  overview STRING,
  release_date DATE,
  genre_ids STRING,
  data_source STRING not null,
  load_time TIMESTAMP not null)
  as
      select distinct movie_id, title, original_language, overview, release_date, genre_ids, data_source, load_time
      from {stg_dataset_name}.tmdb_movie""")

create_Tmdb_Movie_stg = BigQueryInsertJobOperator(
    task_id="create_Tmdb_Movie_stg",
    configuration={
        "query": {
            "query": Tmdb_Movie_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


#Movie_History
movie_history_sql = (
f"""CREATE OR REPLACE TABLE {stg_dataset_name}.Movie_History(
  record_date TIMESTAMP not null,
  movie_id INT64 not null,
  popularity FLOAT64,
  vote_average FLOAT64,
  vote_count INT64,
  data_source STRING not null,
  load_time TIMESTAMP not null)
  as
      select record_date, movie_id, popularity, vote_average, vote_count, data_source, load_time
      from {stg_dataset_name}.tmdb_movie""")

create_movie_history_stg = BigQueryInsertJobOperator(
    task_id="create_movie_history_stg",
    configuration={
        "query": {
            "query": movie_history_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


#tmdb_imdb (intermediate)
tmdb_imdb_sql = (
f"""CREATE OR REPLACE TABLE {stg_dataset_name}.tmdb_imdb(
  tmdb_id INT64 not null,
  imdb_id STRING,
  tmdb_title STRING,
  original_language STRING,
  overview STRING,
  release_date DATE,
  tmdb_genre_ids STRING,
  tmdb_load_time TIMESTAMP,
  imdb_title STRING,
  imdb_release_year INT64,
  imdb_runtime_minutes INT64,
  directors STRING,
  writers STRING,
  data_source STRING not null,
  load_time TIMESTAMP not null)
  as
      SELECT 
            t.tmdb_id AS tmdb_id,
            t.imdb_id AS imdb_id,
            tm.title AS tmdb_title,
            tm.original_language as original_language,
            tm.overview as overview,
            tm.release_date as release_date,
            tm.genre_ids as tmdb_genre_ids,
            tm.load_time as tmdb_load_time,
            im.title AS imdb_title,
            im.release_year AS imdb_release_year,
            im.runtime_minutes AS imdb_runtime_minutes,
            im.directors as directors,
            im.writers as writers,
            'tmdb_imdb' AS data_source,
            tm.load_time as load_time
          FROM 
            {raw_dataset_name}.tmdb_external_ids t
          JOIN 
            {stg_dataset_name}.Tmdb_Movie tm ON t.tmdb_id = tm.movie_id
          JOIN 
            {raw_dataset_name}.imdb_movie im ON t.imdb_id = im.tconst
      """)

create_tmdb_imdb_stg = BigQueryInsertJobOperator(
    task_id="create_tmdb_imdb_stg",
    configuration={
        "query": {
            "query": tmdb_imdb_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

#Movie
movie_sql = (
f"""CREATE OR REPLACE TABLE {stg_dataset_name}.Movie(
  tmdb_id INT64 not null,
  imdb_id STRING,
  title STRING,
  original_language STRING,
  overview STRING,
  release_date DATE,
  genre_ids STRING,
  duration INT64,
  directors STRING,
  writers STRING,
  data_source STRING not null,
  load_time TIMESTAMP not null)
  as
      select distinct t.movie_id as tmdb_id, ti.imdb_id as imdb_id, t.title AS title, t.original_language as original_language,t.overview as overview,t.release_date as release_date,t.genre_ids as genre_ids, ti.imdb_runtime_minutes as duration, ti.directors as directors, ti.writers as writers, t.data_source, t.load_time
      from {stg_dataset_name}.Tmdb_Movie t
      left join {stg_dataset_name}.tmdb_imdb ti
      on ti.tmdb_id = t.movie_id""")

create_movie_stg = BigQueryInsertJobOperator(
    task_id="create_movie_stg",
    configuration={
        "query": {
            "query": movie_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# delete tmdb_movie (intermediate table)
delete_tmdb_movie_stg = BigQueryDeleteTableOperator(
    task_id="delete_tmdb_movie_stg",
    deletion_dataset_table=f"{project_id}.{stg_dataset_name}.tmdb_movie",
    location=region,
    dag=dag)

# delete Tmdb_Movie (intermediate table)
delete_Tmdb_Movie_stg = BigQueryDeleteTableOperator(
    task_id="delete_Tmdb_Movie_stg",
    deletion_dataset_table=f"{project_id}.{stg_dataset_name}.Tmdb_Movie",
    location=region,
    dag=dag)




# update data_source in Movie    
data_source_sql = (
f"""
    update {stg_dataset_name}.Movie set data_source = 'tmdb_imdb' 
    WHERE duration IS NOT NULL 
        OR directors IS NOT NULL 
        OR writers IS NOT NULL
"""
)

update_data_source = BigQueryInsertJobOperator(
    task_id="update_data_source",
    configuration={
        "query": {
            "query": data_source_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# delete tmdb_imdb (intermediate table)
delete_tmdb_imdb_stg = BigQueryDeleteTableOperator(
    task_id="delete_tmdb_imdb_stg",
    deletion_dataset_table=f"{project_id}.{stg_dataset_name}.tmdb_imdb",
    location=region,
    dag=dag)

# create Movie_Service and Snack_Service  
create_movie_snack_service_stg = TriggerDagRunOperator(
    task_id="create_movie_snack_service_stg",
    trigger_dag_id="create-movie-snack-service",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name},
    dag=dag)

# create person
create_person_stg = TriggerDagRunOperator(
    task_id="create_person_stg",
    trigger_dag_id="create-person",  
    conf={"project_id": project_id, "region": region, "raw_dataset_name": raw_dataset_name, "stg_dataset_name": stg_dataset_name},
    dag=dag)

# create Person
Person_sql = (
f"""
create or replace table {stg_dataset_name}.Person( 
    person_id INT64 not null,
    name STRING not null,
    also_known_as STRING,
    biography STRING,
    birth_day INT64,
    birth_month INT64,
    birth_year INT64,
    death_day INT64,
    death_month INT64,
    death_year INT64,
    gender INT64,
    homepage STRING,
    place_of_birth STRING,
    imdb_id STRING,
    known_for_department STRING,
    popularity FLOAT64,
    data_source STRING not null,
    load_time TIMESTAMP not null)
    as
        select *
        from {stg_dataset_name}.person
"""
)

create_Person_stg = BigQueryInsertJobOperator(
    task_id="create_Person_stg",
    configuration={
        "query": {
            "query": Person_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

start >> create_dataset_stg >> create_person_stg >> create_Person_stg >> end
start >> create_dataset_stg >> create_theater_stg >> join
start >> create_dataset_stg >> create_snack_stg >> join
start >> create_dataset_stg >> create_actor_stg >> end
start >> create_dataset_stg >> create_character_stg >> end
start >> create_dataset_stg >> create_crew_stg >> end
start >> create_dataset_stg >> create_genre_stg >> end
start >> create_dataset_stg >> create_movie_genre_stg >> end
start >> create_dataset_stg >> create_tmdb_movie_stg >> create_Tmdb_Movie_stg >> create_movie_history_stg >> create_tmdb_imdb_stg >> create_movie_stg >> update_data_source >> delete_tmdb_movie_stg >> delete_Tmdb_Movie_stg >> delete_tmdb_imdb_stg >> join

join >> create_movie_snack_service_stg >> end