import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

project_id = 'auspicious-lead-382321'
stg_dataset_name = 'movie_stg_af'
csp_dataset_name = 'movie_csp_af'
region = 'us-central1'

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

dag = DAG(
    dag_id='target-controller',
    default_args=default_args,
    description='target table controller dag',
    schedule_interval=None,
    max_active_runs=1,
    concurrency=3,
    catchup=False,
    dagrun_timeout=None,
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)


# create csp dataset
create_dataset_csp = BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset_csp',
    project_id=project_id,
    dataset_id=csp_dataset_name,
    location=region,
    if_exists='ignore',
    dag=dag)

# create Actor
actor_sql = (
f"""
create or replace table {csp_dataset_name}.Actor(
    credit_id STRING not null,
    person_id INT64 not null,
    movie_id INT64 not null,
    name STRING not null,
    character STRING,
    gender INT64,
    credit_order INT64,
    data_source STRING not null,
    load_time TIMESTAMP not null,
    effective_time TIMESTAMP default current_timestamp() not null,
    discontinue_time TIMESTAMP,
    status_flag BOOL not null,
    primary key (credit_id, effective_time) not enforced)
    as select *, current_timestamp, null, true
    from {stg_dataset_name}.Actor
""")

create_actor_csp = BigQueryInsertJobOperator(
    task_id="create_actor_csp",
    configuration={
        "query": {
            "query": actor_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# create Character
character_sql = (
f"""
create or replace table {csp_dataset_name}.Character(
    credit_id STRING not null,
    person_id INT64 not null,
    movie_id INT64 not null,
    name STRING not null,
    character STRING not null,
    credit_order INT64 not null,
    data_source STRING not null,
    load_time TIMESTAMP not null,
    effective_time TIMESTAMP default current_timestamp() not null,
    discontinue_time TIMESTAMP,
    status_flag BOOL not null,
    primary key (credit_id, effective_time) not enforced)
    as select *, current_timestamp(), null, true
    from {stg_dataset_name}.Character
"""
)

create_character_csp = BigQueryInsertJobOperator(
    task_id="create_character_csp",
    configuration={
        "query": {
            "query": character_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# create Crew
crew_sql = (
f"""
create or replace table {csp_dataset_name}.Crew(
    credit_id STRING not null,
    person_id INT64 not null,
    movie_id INT64 not null,
    name STRING not null,
    gender INT64 not null,
    department STRING,
    known_for_department STRING,
    job STRING,
    data_source STRING not null,
    load_time TIMESTAMP not null,
    effective_time TIMESTAMP default current_timestamp() not null,
    discontinue_time TIMESTAMP,
    status_flag BOOL not null,
    primary key (credit_id, effective_time) not enforced)
    as select *, current_timestamp(), null, true
    from {stg_dataset_name}.Crew
"""
)

create_crew_csp = BigQueryInsertJobOperator(
    task_id="create_crew_csp",
    configuration={
        "query": {
            "query": crew_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


# create Genre
genre_sql = (
f"""
create or replace table {csp_dataset_name}.Genre(
    genre_id INT64 not null,
    name STRING not null,
    data_source STRING not null,
    load_time TIMESTAMP not null,
    effective_time TIMESTAMP default current_timestamp() not null,
    discontinue_time TIMESTAMP,
    status_flag BOOL not null,
    primary key (genre_id, effective_time) not enforced)
    as select *, current_timestamp(), null, true
    from {stg_dataset_name}.Genre
"""
)

create_genre_csp = BigQueryInsertJobOperator(
    task_id="create_genre_csp",
    configuration={
        "query": {
            "query": genre_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

    
# create Movie
movie_sql = (
f"""
create or replace table {csp_dataset_name}.Movie(
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
    data_source	STRING not null,
    load_time TIMESTAMP not null,
    effective_time TIMESTAMP default current_timestamp() not null,
    discontinue_time TIMESTAMP,
    status_flag BOOL not null,
    primary key (tmdb_id, effective_time) not enforced)
    as select *, current_timestamp(), null, true
    from {stg_dataset_name}.Movie;
"""
)

create_movie_csp = BigQueryInsertJobOperator(
    task_id="create_movie_csp",
    configuration={
        "query": {
            "query": movie_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# create Movie_Genre
movie_genre_sql = (
f"""
create or replace table {csp_dataset_name}.Movie_Genre(
    movie_genre_id STRING not null,
    movie_id INT64 not null,
    genre_id INT64 not null,
    data_source STRING not null,
    load_time TIMESTAMP not null,
    effective_time TIMESTAMP default current_timestamp() not null,
    discontinue_time TIMESTAMP,
    status_flag BOOL not null,
    primary key (movie_genre_id, effective_time) not enforced)
    as select *, current_timestamp(), null, true
    from {stg_dataset_name}.Movie_Genre;
"""
)

create_movie_genre_csp = BigQueryInsertJobOperator(
    task_id="create_movie_genre_csp",
    configuration={
        "query": {
            "query": movie_genre_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# create Movie_History
movie_history_sql = (
f"""
create or replace table {csp_dataset_name}.Movie_History(
    record_date TIMESTAMP not null,
    movie_id INT64 not null,
    popularity FLOAT64,
    vote_average FLOAT64,
    vote_count INT64,
    data_source STRING not null,
    load_time TIMESTAMP not null,
    effective_time TIMESTAMP default current_timestamp() not null,
    discontinue_time TIMESTAMP,
    status_flag BOOL not null,
    primary key (record_date, movie_id, effective_time) not enforced)
    as select *, current_timestamp(), null, true
    from {stg_dataset_name}.Movie_History;
"""
)

create_movie_history_csp = BigQueryInsertJobOperator(
    task_id="create_movie_history_csp",
    configuration={
        "query": {
            "query": movie_history_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# create Movie_Service
movie_service_sql = (
f"""
create or replace table {csp_dataset_name}.Movie_Service(
    service_id STRING not null,
    movie_id INT64 not null,
    service_type STRING,
    theater_id INT64,
    data_source STRING not null,
    load_time TIMESTAMP not null,
    effective_time TIMESTAMP default current_timestamp() not null,
    discontinue_time TIMESTAMP,
    status_flag BOOL not null,
    primary key (service_id, effective_time) not enforced)
    as select *, current_timestamp(), null, true
    from {stg_dataset_name}.Movie_Service
"""
)

create_movie_service_csp = BigQueryInsertJobOperator(
    task_id="create_movie_service_csp",
    configuration={
        "query": {
            "query": movie_service_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# create Person
person_sql = (
f"""
create or replace table {csp_dataset_name}.Person(
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
    load_time TIMESTAMP not null,
    effective_time TIMESTAMP default current_timestamp() not null,
    discontinue_time TIMESTAMP,
    status_flag BOOL not null,
    primary key (person_id, effective_time) not enforced)
    as select *, current_timestamp(), null, true
    from {stg_dataset_name}.Person
"""
)

create_person_csp = BigQueryInsertJobOperator(
    task_id="create_person_csp",
    configuration={
        "query": {
            "query": person_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# create Snack
snack_sql = (
f"""
create or replace table {csp_dataset_name}.Snack(
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
    load_time TIMESTAMP not null,
    effective_time TIMESTAMP default current_timestamp() not null,
    discontinue_time TIMESTAMP,
    status_flag BOOL not null,
    primary key (id, effective_time) not enforced)
    as select *, current_timestamp(), null, true
    from {stg_dataset_name}.Snack
"""
)

create_snack_csp = BigQueryInsertJobOperator(
    task_id="create_snack_csp",
    configuration={
        "query": {
            "query": snack_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)
    
    
# create Snack_Service
snack_service_sql = (
f"""
create or replace table {csp_dataset_name}.Snack_Service(
    theater_name STRING not null,
    snack_id INT64 not null,
    theater_id INT64 not null,
    theater_chain_name STRING not null,
    service_category STRING,
    data_source STRING not null,
    load_time TIMESTAMP not null,
    effective_time TIMESTAMP default current_timestamp() not null,
    discontinue_time TIMESTAMP,
    status_flag BOOL not null,
    primary key (snack_id, theater_id, effective_time) not enforced)
    as select *, current_timestamp(), null, true
    from {stg_dataset_name}.Snack_Service
"""
)

create_snack_service_csp = BigQueryInsertJobOperator(
    task_id="create_snack_service_csp",
    configuration={
        "query": {
            "query": snack_service_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# create Theater
theater_sql = (
f"""
create or replace table {csp_dataset_name}.Theater(
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
    load_time TIMESTAMP not null,
    effective_time TIMESTAMP default current_timestamp() not null,
    discontinue_time TIMESTAMP,
    status_flag BOOL not null,
    primary key (id, effective_time) not enforced)
    as select *, current_timestamp(), null, true
    from {stg_dataset_name}.Theater
"""
)

create_theater_csp = BigQueryInsertJobOperator(
    task_id="create_theater_csp",
    configuration={
        "query": {
            "query": theater_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


start >> create_dataset_csp >> create_actor_csp >> end
start >> create_dataset_csp >> create_character_csp >> end
start >> create_dataset_csp >> create_crew_csp >> end
start >> create_dataset_csp >> create_genre_csp >> end
start >> create_dataset_csp >> create_movie_csp >> end
start >> create_dataset_csp >> create_movie_genre_csp >> end
start >> create_dataset_csp >> create_movie_history_csp >> end
start >> create_dataset_csp >> create_movie_service_csp >> end
start >> create_dataset_csp >> create_person_csp >> end
start >> create_dataset_csp >> create_snack_csp >> end
start >> create_dataset_csp >> create_snack_service_csp >> end
start >> create_dataset_csp >> create_theater_csp >> end