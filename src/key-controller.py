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
region = 'us-central1'

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

dag = DAG(
    dag_id='key-controller',
    default_args=default_args,
    description='key controller dag',
    schedule_interval=None,
    max_active_runs=1,
    concurrency=3,
    catchup=False,
    dagrun_timeout=None,
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
join = DummyOperator(task_id="join", dag=dag)

#Actor PK
table_name = "Actor"
pk_columns = ["credit_id"]

actor_pk = TriggerDagRunOperator(
    task_id="actor_pk",
    trigger_dag_id="create-pk",
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)

#Character PK
table_name = "Character"
pk_columns = ["credit_id"]

character_pk = TriggerDagRunOperator(
    task_id="character_pk",
    trigger_dag_id="create-pk",
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)

#Crew PK
table_name = "Crew"
pk_columns = ["credit_id"]

crew_pk = TriggerDagRunOperator(
    task_id="crew_pk",
    trigger_dag_id="create-pk",
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)

#Genre PK
table_name = "Genre"
pk_columns = ["genre_id"]

genre_pk = TriggerDagRunOperator(
    task_id="genre_pk",
    trigger_dag_id="create-pk",
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)

#Movie PK
table_name = "Movie"
pk_columns = ["tmdb_id"]

movie_pk = TriggerDagRunOperator(
    task_id="movie_pk",
    trigger_dag_id="create-pk",
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)

#Movie_Genre PK
table_name = "Movie_Genre"
pk_columns = ["movie_genre_id"]

movie_genre_pk = TriggerDagRunOperator(
    task_id="movie_genre_pk",
    trigger_dag_id="create-pk",
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)

#Movie_History PK
table_name = "Movie_History"
pk_columns = ["record_date", "movie_id"]

movie_history_pk = TriggerDagRunOperator(
    task_id="movie_history_pk",
    trigger_dag_id="create-pk",
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)

#Movie_Service PK
table_name = "Movie_Service"
pk_columns = ["service_id"]

movie_service_pk = TriggerDagRunOperator(
    task_id="movie_service_pk",
    trigger_dag_id="create-pk",
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)

#Person PK
table_name = "Person"
pk_columns = ["person_id"]

person_pk = TriggerDagRunOperator(
    task_id="person_pk",
    trigger_dag_id="create-pk",
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)

#Snack PK
table_name = "Snack"
pk_columns = ["id"]

snack_pk = TriggerDagRunOperator(
    task_id="snack_pk",
    trigger_dag_id="create-pk",
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)

#Snack_Service PK
table_name = "Snack_Service"
pk_columns = ["snack_id", "theater_id"]

snack_service_pk = TriggerDagRunOperator(
    task_id="snack_service_pk",
    trigger_dag_id="create-pk",
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)

#Theater PK
table_name = "Theater"
pk_columns = ["id"]

theater_pk = TriggerDagRunOperator(
    task_id="theater_pk",
    trigger_dag_id="create-pk",
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, "table_name": table_name, "pk_columns": pk_columns},
    dag=dag)
    
#######################################################FOREIGN KEYS########################################################################

# Movie_Genre FK #1
child_table_name = "Movie_Genre"
fk_columns = ["genre_id"]
parent_table_name = "Genre"
pk_columns = ["genre_id"]

movie_genre_genre_id_fk = TriggerDagRunOperator(
    task_id="movie_genre_genre_id_fk",
    trigger_dag_id="create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns},
    dag=dag)
  

# Movie_Genre FK #2
child_table_name = "Movie_Genre"
fk_columns = ["movie_id"]
parent_table_name = "Movie"
pk_columns = ["tmdb_id"]

movie_genre_movie_id_fk = TriggerDagRunOperator(
    task_id="movie_genre_movie_id_fk",
    trigger_dag_id="create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns},
    dag=dag)
   
    
# Movie_History FK
child_table_name = "Movie_History"
fk_columns = ["movie_id"]
parent_table_name = "Movie"
pk_columns = ["tmdb_id"]

movie_history_fk = TriggerDagRunOperator(
    task_id="movie_history_fk",
    trigger_dag_id="create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns},
    dag=dag)
   
    
# Crew FK #1
child_table_name = "Crew"
fk_columns = ["movie_id"]
parent_table_name = "Movie"
pk_columns = ["tmdb_id"]

crew_movie_id_fk = TriggerDagRunOperator(
    task_id="crew_movie_id_fk",
    trigger_dag_id="create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns},
    dag=dag)
  
# Crew FK #2
child_table_name = "Crew"
fk_columns = ["person_id"]
parent_table_name = "Person"
pk_columns = ["person_id"]

crew_person_id_fk = TriggerDagRunOperator(
    task_id="crew_person_id_fk",
    trigger_dag_id="create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns},
    dag=dag)    
    
    
# Character FK #1
child_table_name = "Character"
fk_columns = ["movie_id"]
parent_table_name = "Movie"
pk_columns = ["tmdb_id"]

character_movie_id_fk = TriggerDagRunOperator(
    task_id="character_movie_id_fk",
    trigger_dag_id="create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns},
    dag=dag)

# Character FK #2
child_table_name = "Character"
fk_columns = ["credit_id"]
parent_table_name = "Actor"
pk_columns = ["credit_id"]

character_credit_id_fk = TriggerDagRunOperator(
    task_id="character_credit_id_fk",
    trigger_dag_id="create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns},
    dag=dag)


# Actor FK
child_table_name = "Actor"
fk_columns = ["person_id"]
parent_table_name = "Person"
pk_columns = ["person_id"]

actor_fk = TriggerDagRunOperator(
    task_id="actor_fk",
    trigger_dag_id="create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns},
    dag=dag)    
    


    
# Movie_Service FK #1
child_table_name = "Movie_Service"
fk_columns = ["movie_id"]
parent_table_name = "Movie"
pk_columns = ["tmdb_id"]

movie_service_movie_id_fk = TriggerDagRunOperator(
    task_id="movie_service_movie_id_fk",
    trigger_dag_id="create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns},
    dag=dag)
    
    
# Movie_Service FK #2
child_table_name = "Movie_Service"
fk_columns = ["theater_id"]
parent_table_name = "Theater"
pk_columns = ["id"]

movie_service_theater_id_fk = TriggerDagRunOperator(
    task_id="movie_service_theater_id_fk",
    trigger_dag_id="create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns},
    dag=dag)
    
    
# Snack_Service FK #1
child_table_name = "Snack_Service"
fk_columns = ["snack_id"]
parent_table_name = "Snack"
pk_columns = ["id"]

snack_service_snack_id_fk = TriggerDagRunOperator(
    task_id="snack_service_snack_id_fk",
    trigger_dag_id="create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns, },
    dag=dag)


# Snack_Service FK #2
child_table_name = "Snack_Service"
fk_columns = ["theater_id"]
parent_table_name = "Theater"
pk_columns = ["id"]

snack_service_theater_id_fk = TriggerDagRunOperator(
    task_id="snack_service_theater_id_fk",
    trigger_dag_id="create-fk",  
    conf={"project_id": project_id, "region": region, "stg_dataset_name": stg_dataset_name, \
            "child_table_name": child_table_name, "fk_columns": fk_columns, \
            "parent_table_name": parent_table_name, "pk_columns": pk_columns},
    dag=dag)
    

# Primary Keys
start >> actor_pk >> join
start >> character_pk >> join
start >> crew_pk >> join
start >> genre_pk >> join
start >> movie_pk >> join
start >> movie_genre_pk >> join
start >> movie_history_pk >> join
start >> movie_service_pk >> join
start >> person_pk >> join
start >> snack_pk >> join
start >> snack_service_pk >> join
start >> theater_pk >> join

# Foreign Keys
join >> movie_genre_genre_id_fk >> end
join >> movie_genre_movie_id_fk >> end
join >> movie_history_fk >> end
join >> crew_movie_id_fk >> end
join >> crew_person_id_fk >> end
join >> character_movie_id_fk >> end
join >> character_credit_id_fk >> end
join >> actor_fk >> end
join >> movie_service_movie_id_fk >> end
join >> movie_service_theater_id_fk >> end
join >> snack_service_snack_id_fk >> end
join >> snack_service_theater_id_fk >> end
   