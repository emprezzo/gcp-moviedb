"""A dag that creates the primary key on a table."""
import airflow
from airflow import models
from google.cloud import bigquery
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import task
from airflow import AirflowException

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}


@task(trigger_rule="all_done")
def _create_pk(**kwargs):
    
    project_id = kwargs["dag_run"].conf["project_id"]
    region = kwargs["dag_run"].conf["region"]
    stg_dataset_name = kwargs["dag_run"].conf["stg_dataset_name"]
    table_name = kwargs["dag_run"].conf["table_name"]
    pk_columns = kwargs["dag_run"].conf["pk_columns"]
    
    bq_client = bigquery.Client(project=project_id, location=region)
    
    # check to see if the primary key exists before trying to create it
    check_sql = f"""
    select count(*) as count
    from {stg_dataset_name}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS
    where table_name = '{table_name}'
    and constraint_type = 'PRIMARY KEY'
    """
    
    print(check_sql)
    
    try:
        query_job = bq_client.query(check_sql)
        
        for row in query_job:
            count = row["count"]
        
        if count > 0:
            print("primary key exists")
            return 0
            
        print("primary key doesn't exist")    
        alter_sql = f"alter table {stg_dataset_name}.{table_name} add primary key (" + ",".join(pk_columns) + ") not enforced"
        print(alter_sql)
        alter_job = bq_client.query(alter_sql)
        alter_job.result()

        if alter_job.errors:
          print('job errors:', alter_job.errors)
          raise AirflowException("Error in _create_pk")  

    except Exception as e:
        print("Error running SQL: {}".format(e))
        raise AirflowException("Error in _create_pk")        
    

@task(trigger_rule="all_done")
def _check_pk(**kwargs):
    
    project_id = kwargs["dag_run"].conf["project_id"]
    region = kwargs["dag_run"].conf["region"]
    stg_dataset_name = kwargs["dag_run"].conf["stg_dataset_name"]
    table_name = kwargs["dag_run"].conf["table_name"]
    pk_columns = kwargs["dag_run"].conf["pk_columns"]
        
    bq_client = bigquery.Client(project=project_id, location=region)

    # check to see if we have any duplicate PK values
    sql = f"""select count from (select """  
    sql += ", ".join(pk_columns)
    sql += f""", count(*) as count
    from {stg_dataset_name}.{table_name}
    group by """ 
    sql += ", ".join(pk_columns) 
    sql += ") order by count desc limit 1"
            
    print(sql)
    
    try:
        query_job = bq_client.query(sql)
        
        for row in query_job:
            count = row["count"]
        
        if count > 1:
            print("Error: duplicate PK values exist")
            raise AirflowException("Error in _check_pk")
            
        print(f"primary key checks passed on {table_name}.{pk_columns}")    
    
    except Exception as e:
        print("Error running SQL: {}".format(e))
        raise AirflowException("Error in _check_pk")

    
  
with models.DAG(
    "create-pk",
    schedule_interval=None,
    default_args=default_args,
    render_template_as_native_obj=True,
):
    create_pk = _create_pk()
    check_pk = _check_pk()

    create_pk
    check_pk