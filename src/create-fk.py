"""A dag that creates the foreign keys on a table and checks for referential integrity."""
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
def _create_fk(**kwargs):
    
    project_id = kwargs["dag_run"].conf["project_id"]
    region = kwargs["dag_run"].conf["region"]
    stg_dataset_name = kwargs["dag_run"].conf["stg_dataset_name"]
    parent_table_name = kwargs["dag_run"].conf["parent_table_name"]
    child_table_name = kwargs["dag_run"].conf["child_table_name"]
    pk_columns = kwargs["dag_run"].conf["pk_columns"]
    fk_columns = kwargs["dag_run"].conf["fk_columns"]
    
    bq_client = bigquery.Client(project=project_id, location=region)
    
    # check to see if the foreign key exists before trying to create it
    check_sql = f"""
    select count(*) as count
    from {stg_dataset_name}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS
    where table_name = '{child_table_name}'
    and constraint_type = 'FOREIGN KEY'
    """
    
    print(check_sql)
    
    try:
        query_job = bq_client.query(check_sql)
        
        for row in query_job:
            count = row["count"]
        
        if count > 0:
            print("foreign key exists")
            return
            
        print("foreign key doesn't exist")    
        
        alter_sql = f"""
        alter table {stg_dataset_name}.{child_table_name} 
        add foreign key (""" + ",".join(fk_columns) + f""") references {stg_dataset_name}.{parent_table_name}
        (""" + ",".join(pk_columns) + f""") not enforced
        """
        print(alter_sql)
        alter_job = bq_client.query(alter_sql)
        alter_job.result()

        if alter_job.errors:
          print('job errors:', alter_job.errors)
          raise AirflowException("Error in _check_fk")
          

    except Exception as e:
        print("Error running SQL: {}".format(e))
        raise AirflowException("Error in _check_fk")
         

@task(trigger_rule="all_done")
def _check_ref_integrity(**kwargs):
    
    project_id = kwargs["dag_run"].conf["project_id"]
    region = kwargs["dag_run"].conf["region"]
    stg_dataset_name = kwargs["dag_run"].conf["stg_dataset_name"]
    parent_table_name = kwargs["dag_run"].conf["parent_table_name"]
    child_table_name = kwargs["dag_run"].conf["child_table_name"]
    pk_columns = kwargs["dag_run"].conf["pk_columns"]
    fk_columns = kwargs["dag_run"].conf["fk_columns"]
        
    bq_client = bigquery.Client(project=project_id, location=region)

    # check to see if we have any orphan records (i.e. values in the FK column which don't exist in the PK column)
    sql = f"""
    select count(*) as count
    from {stg_dataset_name}.{child_table_name}"""
    
    for i, fk_column in enumerate(fk_columns):
        
        if i == 0:
            sql += f" where {fk_column} not in (select {pk_columns[i]} from {stg_dataset_name}.{parent_table_name})"
        else:
            sql += f" or {fk_column} not in (select {pk_columns[i]} from {stg_dataset_name}.{parent_table_name})"
        
    print(sql)
    
    try:
        query_job = bq_client.query(sql)
        
        for row in query_job:
            count = row["count"]
        
        if count > 0:
            print("Error: orphan records exist")
            raise AirflowException("Error in _check_ref_integrity")
            
        print(f"referential integrity checks passed on {child_table_name}.{fk_columns}")    
    
    except Exception as e:
        print("Error running SQL: {}".format(e))
        raise AirflowException("Error in _check_ref_integrity")



with models.DAG(
    "create-fk",
    schedule_interval=None,
    default_args=default_args,
    render_template_as_native_obj=True,
):
    create_fk = _create_fk()
    check_ref_integrity = _check_ref_integrity()