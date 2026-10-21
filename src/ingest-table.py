"""A dag that ingests all the raw data into BQ."""
import airflow
from airflow import models
from google.cloud import bigquery
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import task

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

@task(trigger_rule="all_done")
def _create_table(**kwargs):
    project_id = kwargs["dag_run"].conf["project_id"]
    bucket_name = kwargs["dag_run"].conf["bucket_name"]
    file_name = kwargs["dag_run"].conf["file_name"]
    dataset_name = kwargs["dag_run"].conf["dataset_name"]
    table_name = kwargs["dag_run"].conf["table_name"]
    schema_full = kwargs["dag_run"].conf["schema_full"]
    schema_formatted = []
    
    for field in schema_full:
        
        if field['name'] == 'load_time':
            schema_formatted.append(bigquery.SchemaField(field['name'], field['type'], mode=field['mode'], default_value_expression=field['default_value_expression']))
        else:
            schema_formatted.append(bigquery.SchemaField(field['name'], field['type'], mode=field['mode']))
    
    print('schema_formatted:', schema_formatted)
    
    table_id = "{}.{}.{}".format(project_id, dataset_name, table_name)
    bq_client = bigquery.Client()
    
    table = bigquery.Table(table_id, schema=schema_formatted)
    created_table = bq_client.create_table(table, exists_ok=True)
    print("Created table {}".format(created_table.table_id))
    
@task(trigger_rule="all_done")
def _load_table(**kwargs):
    project_id = kwargs["dag_run"].conf["project_id"]
    bucket_name = kwargs["dag_run"].conf["bucket_name"]
    file_name = kwargs["dag_run"].conf["file_name"]
    delimiter = kwargs["dag_run"].conf["delimiter"]
    skip_leading_rows = kwargs["dag_run"].conf["skip_leading_rows"]
    dataset_name = kwargs["dag_run"].conf["dataset_name"]
    table_name = kwargs["dag_run"].conf["table_name"]
    schema = kwargs["dag_run"].conf["schema"]
   
    job_config = bigquery.LoadJobConfig(
          schema=schema,
          skip_leading_rows=skip_leading_rows,
          source_format=bigquery.SourceFormat.CSV,
          write_disposition='WRITE_TRUNCATE',
          field_delimiter=delimiter,
          allow_jagged_rows=True,
          allow_quoted_newlines=True,
          ignore_unknown_values=True,
          quote_character='"',
          max_bad_records=10
        )

    bq_client = bigquery.Client()
    uri = "gs://{}/{}".format(bucket_name, file_name)
    table_id = "{}.{}.{}".format(project_id, dataset_name, table_name)
    
    load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()

    destination_table = bq_client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))


with models.DAG(
    "ingest-table",
    schedule_interval=None,
    default_args=default_args,
    render_template_as_native_obj=True,
):
    create_table = _create_table()
    load_table = _load_table()

    # task dependencies
    create_table >> load_table
