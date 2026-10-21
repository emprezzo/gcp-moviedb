"""A dag that ingests all the raw data into BQ."""
import airflow
from airflow import models
from google.cloud import bigquery
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import task
import json, datetime
import random
import uuid

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


def create_table(bq_client, table_id, schema):
    
    table = bigquery.Table(table_id, schema=schema)
    table = bq_client.create_table(table, exists_ok=True)
    print("Created table {}".format(table.table_id))


def load_records(bq_client, table_id, schema, records):

    print('inside load_records: table: ', table_id, ' num records:', str(len(records)))
    print('schema:', schema)
    print('sample record:', records[0])
    
    # load records into staging table
    job_config = bigquery.LoadJobConfig(schema=schema, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, 
                                        write_disposition='WRITE_TRUNCATE')
    table_ref = bigquery.table.TableReference.from_string(table_id)

    try:
        load_job = bq_client.load_table_from_json(records, table_ref, job_config=job_config)
        load_job.result()

        destination_table = bq_client.get_table(target_table_id)
        print("Loaded {} rows.".format(destination_table.num_rows))

        if load_job.errors:
            print('job errors:', load_job.errors)


    except Exception as e:
        print("Error inserting into BQ: {}".format(e))


@task(trigger_rule="all_done")
def _create_movie_service(**kwargs):
    project_id = kwargs["dag_run"].conf["project_id"]
    region = kwargs["dag_run"].conf["region"]
    stg_dataset_name = kwargs["dag_run"].conf["stg_dataset_name"]

    ms_records = []
    theater_ids = []

    stg_table_name = "Movie_Service"
    target_table_id = f"{project_id}.{stg_dataset_name}.{stg_table_name}"

    schema = [bigquery.SchemaField("service_id", "STRING", mode="REQUIRED"),
          bigquery.SchemaField("movie_id", "INTEGER", mode="REQUIRED"),
          bigquery.SchemaField("service_type", "STRING", mode="NULLABLE"),
          bigquery.SchemaField("theater_id", "INTEGER", mode="NULLABLE"),
          bigquery.SchemaField("data_source", "STRING", mode="REQUIRED"),
          bigquery.SchemaField("load_time", "TIMESTAMP", mode="REQUIRED", default_value_expression="CURRENT_TIMESTAMP"),
          ]

    bq_client = bigquery.Client(project=project_id, location=region)
    create_table(bq_client, target_table_id, schema)

    del schema[-1]

    theater_sql = f"select id from {stg_dataset_name}.Theater"
    query_job = bq_client.query(theater_sql)

    for row in query_job:
        theater_ids.append(row["id"])

    # get all popular movies that have a release date within the last 3 weeks
    intheaters_sql = f""" 
    select tmdb_id as movie_id
    from
        (select m.tmdb_id, mh.popularity
        from {stg_dataset_name}.Movie m join {stg_dataset_name}.Movie_History mh
        on m.tmdb_id = mh.movie_id
        where m.release_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 6 WEEK) AND CURRENT_DATE()
        order by mh.popularity DESC)
    """
    query_job = bq_client.query(intheaters_sql)

    index = 0

    # for each in-theater movie, assign it a 1-5 theaters (aka theater_id)
    for row in query_job:
        movie_id = row["movie_id"]
    
        # there are no more theaers to allocate
        if index >= len(theater_ids):
            break
        
        # Determine a random number of theaters for this movie
        num_theaters = random.randint(1, 5)
        # Randomly select theater_ids without replacement to avoid duplicates
        selected_theater_ids = random.sample(theater_ids, num_theaters)    
        # assign number (1-5) of random theaters to the in-theater movie
        for theater_id in selected_theater_ids:
            service_id = str(uuid.uuid4())[:13]
            record = {"service_id":service_id, "movie_id": movie_id, "service_type": "in-theaters", "theater_id": theater_id,
                      "data_source": "tmdb_github"}

            filtered_record = remove_none_values(record)

            ms_records.append(filtered_record)

        index += 1

    #load records to the Movie_Service table
    load_records(bq_client, target_table_id, schema, ms_records)

@task(trigger_rule="all_done")
def _create_snack_service(**kwargs):
    project_id = kwargs["dag_run"].conf["project_id"]
    region = kwargs["dag_run"].conf["region"]
    stg_dataset_name = kwargs["dag_run"].conf["stg_dataset_name"]

    ss_records = [] # snack service records to be inserted
    snack_ids = []

    stg_table_name = "Snack_Service"
    target_table_id = f"{project_id}.{stg_dataset_name}.{stg_table_name}"

    schema = [bigquery.SchemaField("theater_name", "STRING", mode="NULLABLE"),
          bigquery.SchemaField("snack_id", "INTEGER", mode="NULLABLE"),
          bigquery.SchemaField("theater_id", "INTEGER", mode="NULLABLE"),
          bigquery.SchemaField("theater_chain_name", "STRING", mode="NULLABLE"),
          bigquery.SchemaField("service_category", "STRING", mode="NULLABLE"),
          bigquery.SchemaField("data_source", "STRING", mode="REQUIRED"),
          bigquery.SchemaField("load_time", "TIMESTAMP", mode="REQUIRED", default_value_expression="CURRENT_TIMESTAMP"),
          ]

    bq_client = bigquery.Client(project=project_id, location=region)
    create_table(bq_client, target_table_id, schema)      

    del schema[-1]

    snack_sql = f"select id from {stg_dataset_name}.Snack"
    query_job = bq_client.query(snack_sql)

    for row in query_job:
        snack_ids.append(row["id"])

    theater_sql = f"""
    select theaterChainName, ARRAY_AGG(id) as theater_ids, ARRAY_AGG(name) as theater_names
    from {stg_dataset_name}.Theater
    group by theaterChainName
    """

    query_job = bq_client.query(theater_sql)

    index = 0

    for row in query_job:
        theater_chain = row["theaterChainName"]
        theater_ids= row["theater_ids"]
        theater_names = row["theater_names"]

        if index >= len(snack_ids):
            break
        snack_id = snack_ids[index]
        for i in range(len(theater_names)):
            record = {"theater_name": theater_names[i], "snack_id": snack_id, "theater_id": theater_ids[i], "theater_chain_name": theater_chain, "data_source": "amc_github"}
            filtered_record = remove_none_values(record)
            ss_records.append(filtered_record)
        index += 1

    load_records(bq_client,target_table_id, schema, ss_records)
    
with models.DAG(
    "create-movie-snack-service",
    schedule_interval=None,
    default_args=default_args,
    render_template_as_native_obj=True,
):
    create_movie_service = _create_movie_service()
    create_snack_service = _create_snack_service()

    # run both tasks in parallel
    create_movie_service
    create_snack_service
