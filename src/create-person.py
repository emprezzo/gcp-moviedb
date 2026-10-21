"""A dag that ingests all the raw data into BQ."""
import airflow
from airflow import models
from google.cloud import bigquery
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import task
import json, datetime

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

def serialize_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")

# removes the entries from the dictionary whose values are None
# this filter is needed for loading JSON into BQ
def remove_none_values(record):
    filtered_record = {}
    for field in record.keys():
        if record[field] != None:
            filtered_record[field] = record[field]
    return filtered_record

def split_dates(date):
    day = None
    month = None
    year = None 
    if not date:
        pass    
    elif len(date.split("-")) > 1:
        day = int(date.split("-")[2].strip())
        month = int(date.split("-")[1].strip())
        year = int(date.split("-")[0].strip())
    elif len(date.split("/")) > 1:
        day = int(date.split("/")[0].strip())
        month = int(date.split("/")[1].strip())
        year = int(date.split("/")[2].strip())

    return day, month, year


def create_table(bq_client, table_id, schema):
    
    table = bigquery.Table(table_id, schema=schema)
    table = bq_client.create_table(table, exists_ok=True)
    print("Created table {}".format(table.table_id))

    
@task(trigger_rule="all_done")
def _create_person(**kwargs):
    
    project_id = kwargs["dag_run"].conf["project_id"]
    region = kwargs["dag_run"].conf["region"]
    raw_dataset_name = kwargs["dag_run"].conf["raw_dataset_name"]
    stg_dataset_name = kwargs["dag_run"].conf["stg_dataset_name"]
    
    raw_table_name = "tmdb_person_info"
    stg_table_name = "person" # lowercase the name because it's an intermediate table
    
    tmdb_person = []
    target_table_id = "{}.{}.{}".format(project_id, stg_dataset_name, stg_table_name)

    schema = [
        bigquery.SchemaField("person_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("also_known_as", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("biography", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("birth_day", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("birth_month", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("birth_year", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("death_day", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("death_month", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("death_year", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("gender", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("homepage", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("place_of_birth", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("imdb_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("known_for_department", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("popularity", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("data_source", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("load_time", "TIMESTAMP", mode="REQUIRED")
    ]

    bq_client = bigquery.Client(project=project_id, location=region)
    sql = f"select * from {raw_dataset_name}.{raw_table_name}"
    query_job = bq_client.query(sql)
    create_table(bq_client, target_table_id, schema)
    # birthday and deathday strings have format: dd/mm/yyyy OR yyyy-mm-dd
    for row in query_job:
        person_id = row["person_id"]
        name = row["name"]
        also_known_as = row["also_known_as"]
        biography = row["biography"]
        gender = row["gender"]
        homepage = row["homepage"]
        place_of_birth = row["place_of_birth"]
        imdb_id = row["imdb_id"]
        known_for_department = row["known_for_department"]
        popularity = row["popularity"]
        birthday = row["birthday"]
        deathday = row["deathday"]
        load_time = json.dumps(row["load_time"], default=serialize_datetime).replace('"', '')

        birth_day, birth_month, birth_year = split_dates(birthday)
        death_day, death_month, death_year = split_dates(deathday)

        record = {
            "person_id": person_id,
            "name": name,
            "also_known_as": also_known_as,
            "biography": biography,
            "birth_day": birth_day,
            "birth_month": birth_month,
            "birth_year": birth_year,
            "death_day": death_day,
            "death_month": death_month,
            "death_year": death_year,
            "gender": gender,
            "homepage": homepage,
            "place_of_birth": place_of_birth,
            "imdb_id": imdb_id,
            "known_for_department": known_for_department,
            "popularity": popularity,
            "data_source": "tmdb",
            "load_time": load_time
        }
        
        filtered_record = remove_none_values(record)
        tmdb_person.append(filtered_record)
        
    print('inside load_records: table: ', target_table_id, ' num records:', str(len(tmdb_person)))
    print('schema:', schema)
    print('sample record:', tmdb_person[0])
    
    # load the records into staging table
    job_config = bigquery.LoadJobConfig(schema=schema, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, 
                                        write_disposition='WRITE_TRUNCATE')
    table_ref = bigquery.table.TableReference.from_string(target_table_id)

    try:
        load_job = bq_client.load_table_from_json(tmdb_person[:100000], table_ref, job_config=job_config)
        destination_table = bq_client.get_table(target_table_id)
        load_job.result()
        print('Wrote', destination_table.num_rows, 'records into', target_table_id)

        if load_job.errors:
            print('job errors:', load_job.errors)

    except Exception as e:
        print("Error inserting into BQ: {}".format(e))
    
    
with models.DAG(
    "create-person",
    schedule_interval=None,
    default_args=default_args,
    render_template_as_native_obj=True,
):
    create_person = _create_person()

    #task dependencies
    create_person
# """A dag that ingests all the raw data into BQ."""
# import airflow
# from airflow import models
# from google.cloud import bigquery
# from airflow.utils.trigger_rule import TriggerRule
# from airflow.decorators import task
# import json
# import datetime

# default_args = {
#     'start_date': airflow.utils.dates.days_ago(0),
#     'retries': 0
# }

# def serialize_datetime(obj):
#     if isinstance(obj, datetime.datetime):
#         return obj.isoformat()
#     raise TypeError("Type not serializable")

# # removes the entries from the dictionary whose values are None
# # this filter is needed for loading JSON into BQ
# def remove_none_values(record):
#     filtered_record = {}
#     for field in record.keys():
#         if record[field] is not None:
#             filtered_record[field] = record[field]
#     return filtered_record

# def split_dates(date):
#     if not date:
#         return None, None, None
#     elif '-' in date:
#         parts = date.split("-")
#     elif '/' in date:
#         parts = date.split("/")
#     else:
#         return None, None, None

#     day = int(parts[2].strip())
#     month = int(parts[1].strip())
#     year = int(parts[0].strip())
#     return day, month, year

# def create_table(bq_client, table_id, schema):
#     table = bigquery.Table(table_id, schema=schema)
#     table = bq_client.create_table(table, exists_ok=True)
#     print("Created table {}".format(table.table_id))

# @task(trigger_rule="all_done")
# def _create_person(**kwargs):
#     project_id = kwargs["dag_run"].conf["project_id"]
#     region = kwargs["dag_run"].conf["region"]
#     raw_dataset_name = kwargs["dag_run"].conf["raw_dataset_name"]
#     stg_dataset_name = kwargs["dag_run"].conf["stg_dataset_name"]

#     raw_table_name = "tmdb_person_info"
#     stg_table_name = "person"  # lowercase the name because it's an intermediate table

#     target_table_id = f"{project_id}.{stg_dataset_name}.{stg_table_name}"
    
#     schema = [
#         bigquery.SchemaField("person_id", "INTEGER", mode="REQUIRED"),
#         bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
#         bigquery.SchemaField("also_known_as", "STRING", mode="NULLABLE"),
#         bigquery.SchemaField("biography", "STRING", mode="NULLABLE"),
#         bigquery.SchemaField("birth_day", "INTEGER", mode="NULLABLE"),
#         bigquery.SchemaField("birth_month", "INTEGER", mode="NULLABLE"),
#         bigquery.SchemaField("birth_year", "INTEGER", mode="NULLABLE"),
#         bigquery.SchemaField("death_day", "INTEGER", mode="NULLABLE"),
#         bigquery.SchemaField("death_month", "INTEGER", mode="NULLABLE"),
#         bigquery.SchemaField("death_year", "INTEGER", mode="NULLABLE"),
#         bigquery.SchemaField("gender", "INTEGER", mode="NULLABLE"),
#         bigquery.SchemaField("homepage", "STRING", mode="NULLABLE"),
#         bigquery.SchemaField("place_of_birth", "STRING", mode="NULLABLE"),
#         bigquery.SchemaField("imdb_id", "STRING", mode="NULLABLE"),
#         bigquery.SchemaField("known_for_department", "STRING", mode="NULLABLE"),
#         bigquery.SchemaField("popularity", "FLOAT", mode="NULLABLE"),
#         bigquery.SchemaField("data_source", "STRING", mode="REQUIRED"),
#         bigquery.SchemaField("load_time", "TIMESTAMP", mode="REQUIRED")
#     ]

#     bq_client = bigquery.Client(project=project_id, location=region)
#     sql = f"SELECT * FROM {raw_dataset_name}.{raw_table_name}"
#     query_job = bq_client.query(sql)

#     create_table(bq_client, target_table_id, schema)
    
#     # Load records in batches
#     batch_size = 1000
#     tmdb_person_batch = []
#     for row in query_job:
#         tmdb_person_batch.append(row)
#         if len(tmdb_person_batch) >= batch_size:
#             process_batch(tmdb_person_batch, target_table_id, schema, bq_client)
#             tmdb_person_batch = []
    
#     # Process any remaining records
#     if tmdb_person_batch:
#         process_batch(tmdb_person_batch, target_table_id, schema, bq_client)

# def process_batch(records, target_table_id, schema, bq_client):
#     tmdb_person = []
#     for row in records:
#         person_id = row["person_id"]
#         name = row["name"]
#         also_known_as = row["also_known_as"]
#         biography = row["biography"]
#         gender = row["gender"]
#         homepage = row["homepage"]
#         place_of_birth = row["place_of_birth"]
#         imdb_id = row["imdb_id"]
#         known_for_department = row["known_for_department"]
#         popularity = row["popularity"]
#         birthday = row["birthday"]
#         deathday = row["deathday"]
#         load_time = json.dumps(row["load_time"], default=serialize_datetime).replace('"', '')

#         birth_day, birth_month, birth_year = split_dates(birthday)
#         death_day, death_month, death_year = split_dates(deathday)

#         record = {
#             "person_id": person_id,
#             "name": name,
#             "also_known_as": also_known_as,
#             "biography": biography,
#             "birth_day": birth_day,
#             "birth_month": birth_month,
#             "birth_year": birth_year,
#             "death_day": death_day,
#             "death_month": death_month,
#             "death_year": death_year,
#             "gender": gender,
#             "homepage": homepage,
#             "place_of_birth": place_of_birth,
#             "imdb_id": imdb_id,
#             "known_for_department": known_for_department,
#             "popularity": popularity,
#             "data_source": "tmdb",
#             "load_time": load_time
#         }

#         filtered_record = remove_none_values(record)
#         tmdb_person.append(filtered_record)

#     job_config = bigquery.LoadJobConfig(
#         schema=schema,
#         source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
#         write_disposition='WRITE_APPEND'
#     )
#     try:
#         load_job = bq_client.load_table_from_json(
#             tmdb_person, target_table_id, job_config=job_config
#         )
#         destination_table = bq_client.get_table(target_table_id)
#         load_job.result()
#         print('Wrote', destination_table.num_rows, 'records into', target_table_id)

#         if load_job.errors:
#             print('Job errors:', load_job.errors)

#     except Exception as e:
#         print("Error inserting into BQ: {}".format(e))

# with models.DAG(
#     "create-person",
#     schedule_interval=None,
#     default_args=default_args,
#     render_template_as_native_obj=True,
# ) as dag:
#     create_person = _create_person()
#     create_person
