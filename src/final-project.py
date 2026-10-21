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
stg_dataset_name = 'movie_stg_af'
ai_dataset_name = 'movie_stg_ai_af'
bucket_name = 'movie_data_lpez'
region = 'us-central1'
default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

dag = DAG(
    dag_id='enrich',
    default_args=default_args,
    description='controller dag',
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=50),
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
join = DummyOperator(task_id="join", dag=dag)

create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset_if_needed',
    project_id=project_id,
    dataset_id=ai_dataset_name,
    location=region,
    if_exists='ignore',
    dag=dag)


##### PREDICT SNACK CATEGORY #####
predict_category_sql = (f"""create or replace table {ai_dataset_name}.category_predictions_raw as
select * from ML.generate_text(model remote_models_uscentral.gemini_pro,
  (
    select concat("Suggest a category for the snack. If the snack has the word smoothie in its name should be put in the Smoothies category. Return the output as json, include only the id and category in the output. \\n", 
        to_json_string(json_object("id", id, "name", name))) as prompt
    from {stg_dataset_name}.Snack
    order by id
  ),
  struct(TRUE as flatten_json_output)
)""")

predict_category = BigQueryInsertJobOperator(
    task_id="predict_category",
    configuration={
        "query": {
            "query": predict_category_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# Create prediction formatted table
create_category_ai_sql = (f"""create or replace table {ai_dataset_name}.category_predictions_formatted as
    select trim(replace(replace(replace(ml_generate_text_llm_result, '```json', ''), '```', ''), '\\n', '')) as ml_generate_text_llm_result
    from {ai_dataset_name}.category_predictions_raw;""")
create_category_ai = BigQueryInsertJobOperator(
    task_id="create_category_ai",
    configuration={
        "query": {
            "query": create_category_ai_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# Add/Update category
add_column_category_sql = (f"""
    ALTER TABLE {stg_dataset_name}.Snack
    DROP COLUMN category;
    ALTER TABLE {stg_dataset_name}.Snack
    ADD COLUMN category string;""")
add_column_category = BigQueryInsertJobOperator(
    task_id="add_column_category",
    configuration={
        "query": {
            "query": add_column_category_sql,
            "useLegacySql": False,
        }
    },    location=region,    dag=dag)
update_category_sql = (f"""
    update {stg_dataset_name}.Snack set category =
    (select JSON_VALUE(ml_generate_text_llm_result, '$.category')
    from {ai_dataset_name}.category_predictions_formatted
    where CAST(id AS STRING) = json_value(ml_generate_text_llm_result, '$.id'))
    where 1=1""")

update_category = BigQueryInsertJobOperator(
    task_id="update_category",
    configuration={
        "query": {
            "query": update_category_sql,
            "useLegacySql": False,
        }
    },
    location=region,    dag=dag)


##### PREDICT SNACK IS_HEALTHY #####
predict_is_healthy_sql = (f"""create or replace table {ai_dataset_name}.is_healthy_predictions_raw as
select *
from ML.generate_text(
  model remote_models_uscentral.gemini_pro,
  (
    select concat("Detect whether a snack is healthy or unhealthy. A snack should have is_healthy value as false if it has high calories or high sugar or hight total_fat or high sodium or high cholesterol, or a combination of these. Also, a snack should have is_healthy value if it has the category Cookies or Fried. Return the output as json, include only the id and is_healthy in the output.\\n",
         to_json_string(json_object("id", id, "name", name, "category", category,
         "calories", calories, "total_fat", total_fat, "sodium", sodium, "cholesterol", cholesterol))) as prompt
    from {stg_dataset_name}.Snack
    order by id
  ),
  struct(0 as temperature, 8192 as max_output_tokens, 0.0 as top_p, 1 as top_k, TRUE as flatten_json_output)
);""")

predict_is_healthy = BigQueryInsertJobOperator(
    task_id="predict_is_healthy",
    configuration={
        "query": {
            "query": predict_is_healthy_sql,
            "useLegacySql": False,
        }
    },    location=region,    dag=dag)

# Create prediction formatted table
create_is_healthy_ai_sql = (f"""create or replace table {ai_dataset_name}.is_healthy_predictions_formatted as
  select trim(replace(replace(replace(ml_generate_text_llm_result, '```json', ''), '```', ''), '\\n', '')) as ml_generate_text_llm_result
  from {ai_dataset_name}.is_healthy_predictions_raw""")

create_is_healthy_ai = BigQueryInsertJobOperator(
    task_id="create_is_healthy_ai",
    configuration={
        "query": {
            "query": create_is_healthy_ai_sql,
            "useLegacySql": False,
        }
    },    location=region,    dag=dag)

# Add/Update is_healthy
add_column_is_healthy_sql = (f"""ALTER TABLE {stg_dataset_name}.Snack
    DROP COLUMN is_healthy;
    ALTER TABLE {stg_dataset_name}.Snack
    ADD COLUMN is_healthy string;""")
add_column_is_healthy = BigQueryInsertJobOperator(
    task_id="add_column_is_healthy",
    configuration={
        "query": {
            "query": add_column_is_healthy_sql,
            "useLegacySql": False,
        }
    },    location=region,    dag=dag)

update_is_healthy_sql = (f"""update {stg_dataset_name}.Snack set is_healthy =
  (select json_value(ml_generate_text_llm_result, '$.is_healthy')
   from  {ai_dataset_name}.is_healthy_predictions_formatted
   where CAST(id AS STRING) = json_value(ml_generate_text_llm_result, '$.id'))
where 1=1;""")

update_is_healthy = BigQueryInsertJobOperator(
    task_id="update_is_healthy",
    configuration={
        "query": {
            "query": update_is_healthy_sql,
            "useLegacySql": False,
        }
    },    location=region,    dag=dag)


##### ENRICH SNACK CALORIES #####
enrich_calories_sql = (f"""create or replace table {ai_dataset_name}.calories_predictions_raw as
  select *
  from ML.generate_text(
    model remote_models_uscentral.gemini_pro,
    (
      select concat("Suggest a single value for calories for the snack. Snacks that high values for cholesterol or sodium should have higher calories. If the snack already has a value for calories, keep and use that value for that snack. Return the output as json, include only the id and calories in the output.\\n", 
        to_json_string(json_object("id", id, "name", name, "is_healthy", is_healthy, "calories", calories))) as prompt
      from {stg_dataset_name}.Snack
      order by id
    ),
    struct(0 as temperature, 8192 as max_output_tokens, 0.0 as top_p, 1 as top_k, TRUE as flatten_json_output)
  );""")

enrich_calories = BigQueryInsertJobOperator(
    task_id="enrich_calories",
    configuration={
        "query": {
            "query": enrich_calories_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# Create prediction formatted table
create_calories_ai_sql = (f"""create or replace table {ai_dataset_name}.calories_predictions_formatted as
  select trim(replace(replace(replace(ml_generate_text_llm_result, '```json', ''), '```', ''), '\\n', '')) as ml_generate_text_llm_result
  from {ai_dataset_name}.calories_predictions_raw
""")

create_calories_ai = BigQueryInsertJobOperator(
    task_id="create_calories_ai",
    configuration={
        "query": {
            "query": create_calories_ai_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)

# Update calories
update_calories_sql = (f"""update {stg_dataset_name}.Snack set calories =
  (select max(CAST(JSON_VALUE(ml_generate_text_llm_result, '$.calories') AS FLOAT64))
   from {ai_dataset_name}.calories_predictions_formatted
   where CAST(id AS STRING) = json_value(ml_generate_text_llm_result, '$.id'))
where 1=1
""")

update_calories = BigQueryInsertJobOperator(
    task_id="update_calories",
    configuration={
        "query": {
            "query": update_calories_sql,
            "useLegacySql": False,
        }
    },
    location=region,
    dag=dag)


##### ENIRCH TOTAL_FAT IN SNACK #####
enrich_total_fat_sql = (f"""create or replace table {ai_dataset_name}.total_fat_predictions_raw as
  select *
  from ML.generate_text(
    model remote_models_uscentral.gemini_pro,
    (
      select concat("Suggest a total_fat value for the snack. Snacks that have is_healthy as true should have lower total_fat than snacks that have is_healthy as false. Snacks that have higher calories and calories_from_fat have higher total_fat. If the snack already has a value for total_fat, keep and use that value for that snack. Return the output as json, include only the id and total_fat in the output.\\n", 
      to_json_string(json_object("id", id, "name", name, "calories", calories, "calories_from_fat", calories_from_fat, "is_healthy", is_healthy))) as prompt
      from {stg_dataset_name}.Snack
      order by id
    ),
    struct(0 as temperature, 8192 as max_output_tokens, 0.0 as top_p, 1 as top_k, TRUE as flatten_json_output)
  );""")

enrich_total_fat = BigQueryInsertJobOperator(
    task_id="enrich_total_fat_sql",
    configuration={
        "query": {
            "query": enrich_total_fat_sql,
            "useLegacySql": False,
        }
    },    location=region,    dag=dag)

# Create prediction formatted table
create_total_fat_ai_sql = (f"""create or replace table {ai_dataset_name}.total_fat_predictions_formatted as
    select trim(replace(replace(replace(ml_generate_text_llm_result, '```json', ''), '```', ''), '\\n', '')) as ml_generate_text_llm_result
    from {ai_dataset_name}.total_fat_predictions_raw
""")

create_total_fat_ai = BigQueryInsertJobOperator(
    task_id="create_total_fat_ai",
    configuration={
        "query": {
            "query": create_total_fat_ai_sql,
            "useLegacySql": False,
        }
    },    location=region,    dag=dag)

# Update total_fat
update_total_fat_sql = (f"""update {stg_dataset_name}.Snack set total_fat =
    (select max(CAST(JSON_VALUE(ml_generate_text_llm_result, '$.total_fat') AS FLOAT64))
    from {ai_dataset_name}.total_fat_predictions_formatted
    where CAST(id AS STRING) = json_value(ml_generate_text_llm_result, '$.id'))
    where 1=1""")

update_total_fat = BigQueryInsertJobOperator(
    task_id="update_total_fat",
    configuration={
        "query": {
            "query": update_total_fat_sql,
            "useLegacySql": False,
        }
    },    location=region,    dag=dag)




##### ENIRCH satured_fat IN SNACK #####
enrich_satured_fat_sql = (f"""create or replace table {ai_dataset_name}.satured_fat_predictions_raw as
  select *
  from ML.generate_text(
    model remote_models_uscentral.gemini_pro,
    (
      select concat("Suggest a satured_fat value for the snack. For each snack, the sum of satured_fat and trans_fat must not exceed the snack's total_fat value. Snacks that are made from animal-based foods, dairy products, and tropical oils tend to have high satured_fat. If the snack already has a value for satured_fat, keep and use that value for that snack. Return the output as json, include only the 'id' and 'satured_fat' in the output.\\n", 
      to_json_string(json_object("id", id, "name", name, "calories", calories, "calories_from_fat", calories_from_fat, "total_fat", total_fat, "trans_fat", trans_fat, "is_healthy", is_healthy))) as prompt
      from {stg_dataset_name}.Snack
      order by id
    ),
    struct(0 as temperature, 8192 as max_output_tokens, 0.0 as top_p, 1 as top_k, TRUE as flatten_json_output)
  );""")

enrich_satured_fat = BigQueryInsertJobOperator(
    task_id="enrich_satured_fat_sql",
    configuration={
        "query": {
            "query": enrich_satured_fat_sql,
            "useLegacySql": False,
        }
    },    location=region,    dag=dag)

# Create prediction formatted table
create_satured_fat_ai_sql = (f"""create or replace table {ai_dataset_name}.satured_fat_predictions_formatted as
    select trim(replace(replace(replace(ml_generate_text_llm_result, '```json', ''), '```', ''), '\\n', '')) as ml_generate_text_llm_result
    from {ai_dataset_name}.satured_fat_predictions_raw
""")

create_satured_fat_ai = BigQueryInsertJobOperator(
    task_id="create_satured_fat_ai",
    configuration={
        "query": {
            "query": create_satured_fat_ai_sql,
            "useLegacySql": False,
        }
    },    location=region,    dag=dag)

# Update satured_fat
update_satured_fat_sql = (f"""update {stg_dataset_name}.Snack set satured_fat =
    (select max(CAST(JSON_VALUE(ml_generate_text_llm_result, '$.satured_fat') AS FLOAT64))
    from {ai_dataset_name}.satured_fat_predictions_formatted
    where CAST(id AS STRING) = json_value(ml_generate_text_llm_result, '$.id'))
    where 1=1""")

update_satured_fat = BigQueryInsertJobOperator(
    task_id="update_satured_fat",
    configuration={
        "query": {
            "query": update_satured_fat_sql,
            "useLegacySql": False,
        }
    },    location=region,    dag=dag)



##### ENIRCH trans_fat IN SNACK #####
enrich_trans_fat_sql = (f"""create or replace table {ai_dataset_name}.trans_fat_predictions_raw as
  select *
  from ML.generate_text(
    model remote_models_uscentral.gemini_pro,
    (
      select concat("Suggest a trans_fat value for the snack. Snacks in the category of baked goods, fried foods, and shortenings have higher trans_fat. Snacks that have is_healthy as true will have lower trans_fat than snacks with is_healthy as false. If the snack already has a value for trans_fat, keep and use that value for that snack. For each snack, the sum of satured_fat and trans_fat must not exceed the snack's total_fat value. Return the output as json, include only the id and trans_fat in the output.\\n", 
      to_json_string(json_object("id", id, "name", name, "calories", calories, "calories_from_fat", calories_from_fat, "total_fat", total_fat, "trans_fat", trans_fat, "is_healthy", is_healthy))) as prompt
      from {stg_dataset_name}.Snack
      order by id
    ),
    struct(0 as temperature, 8192 as max_output_tokens, 0.0 as top_p, 1 as top_k, TRUE as flatten_json_output)
  );""")

enrich_trans_fat = BigQueryInsertJobOperator(
    task_id="enrich_trans_fat_sql",
    configuration={
        "query": {
            "query": enrich_trans_fat_sql,
            "useLegacySql": False,
        }
    },    location=region,    dag=dag)

# Create prediction formatted table
create_trans_fat_ai_sql = (f"""create or replace table {ai_dataset_name}.trans_fat_predictions_formatted as
    select trim(replace(replace(replace(ml_generate_text_llm_result, '```json', ''), '```', ''), '\\n', '')) as ml_generate_text_llm_result
    from {ai_dataset_name}.trans_fat_predictions_raw
""")

create_trans_fat_ai = BigQueryInsertJobOperator(
    task_id="create_trans_fat_ai",
    configuration={
        "query": {
            "query": create_trans_fat_ai_sql,
            "useLegacySql": False,
        }
    },    location=region,    dag=dag)

# Update trans_fat
update_trans_fat_sql = (f"""update {stg_dataset_name}.Snack set trans_fat =
    (select max(CAST(JSON_VALUE(ml_generate_text_llm_result, '$.trans_fat') AS FLOAT64))
    from {ai_dataset_name}.trans_fat_predictions_formatted
    where CAST(id AS STRING) = json_value(ml_generate_text_llm_result, '$.id'))
    where 1=1""")

update_trans_fat = BigQueryInsertJobOperator(
    task_id="update_trans_fat",
    configuration={
        "query": {
            "query": update_trans_fat_sql,
            "useLegacySql": False,
        }
    },    location=region,    dag=dag)


##### ENIRCH polyunsat_fat IN SNACK #####

enrich_polyunsat_fat_sql = (f"""create or replace table {ai_dataset_name}.polyunsat_fat_predictions_raw as
  select *
  from ML.generate_text(
    model remote_models_uscentral.gemini_pro,
    (
      select concat("Suggest a polyunsat_fat value for the snack. polyunsat_fat is found in plant and animal snacks. Snacks that have is_healthy as true should have higher polyunsat_fat than snacks that have is_healthy as false. For each snack, the sum of its satured_fat, trans_fat, polyunsat_fat, and monounsat_fat must be less than its total_fat value. Snacks that have is_healthy as true will have lower polyunsat_fat than snacks with is_healthy as false. If the snack already has a value for polyunsat_fat, keep and use that value for that snack. Return the output as json, include only the id and polyunsat_fat in the output.\\n", 
      to_json_string(json_object("id", id, "name", name, "calories", calories, "calories_from_fat", calories_from_fat, "total_fat", total_fat, "polyunsat_fat", polyunsat_fat, "is_healthy", is_healthy))) as prompt
      from {stg_dataset_name}.Snack
      order by id
    ),
    struct(0 as temperature, 8192 as max_output_tokens, 0.0 as top_p, 1 as top_k, TRUE as flatten_json_output)
  );""")

enrich_polyunsat_fat = BigQueryInsertJobOperator(
    task_id="enrich_polyunsat_fat_sql",
    configuration={
        "query": {
            "query": enrich_polyunsat_fat_sql,
            "useLegacySql": False,
        }
    },    location=region,    dag=dag)

# Create prediction formatted table
create_polyunsat_fat_ai_sql = (f"""create or replace table {ai_dataset_name}.polyunsat_fat_predictions_formatted as
    select trim(replace(replace(replace(ml_generate_text_llm_result, '```json', ''), '```', ''), '\\n', '')) as ml_generate_text_llm_result
    from {ai_dataset_name}.polyunsat_fat_predictions_raw
""")

create_polyunsat_fat_ai = BigQueryInsertJobOperator(
    task_id="create_polyunsat_fat_ai",
    configuration={
        "query": {
            "query": create_polyunsat_fat_ai_sql,
            "useLegacySql": False,
        }
    },    location=region,    dag=dag)

# Update polyunsat_fat
update_polyunsat_fat_sql = (f"""update {stg_dataset_name}.Snack set polyunsat_fat =
    (select CAST(JSON_VALUE(ml_generate_text_llm_result, '$.polyunsat_fat') AS FLOAT64)
    from {ai_dataset_name}.polyunsat_fat_predictions_formatted
    where CAST(id AS STRING) = json_value(ml_generate_text_llm_result, '$.id'))
    where 1=1""")

update_polyunsat_fat = BigQueryInsertJobOperator(
    task_id="update_polyunsat_fat",
    configuration={
        "query": {
            "query": update_polyunsat_fat_sql,
            "useLegacySql": False,
        }
    },    location=region,    dag=dag)



##### ENIRCH carbohydrates IN SNACK #####

enrich_carbohydrates_sql = (f"""create or replace table {ai_dataset_name}.carbohydrates_predictions_raw as
  select *
  from ML.generate_text(
    model remote_models_uscentral.gemini_pro,
    (
      select concat("Suggest a value for carbohydrates for the snack. Snacks that have a category value of Chips, Cookies, Main Course, Burgers, Pizza, Pastries, Desserts, Fried, Baked Goods, Sandwiches, or Breakfast will have higher carbohydrates value. If the snack already has a value for carbohydrates, keep and use that value for that snack. Return the output as json, include only the id and carbohydrates in the output.\\n", 
      to_json_string(json_object("id", id, "name", name, "calories", calories, "calories_from_fat", calories_from_fat, "total_fat", total_fat, "carbohydrates", carbohydrates, "is_healthy", is_healthy))) as prompt
      from {stg_dataset_name}.Snack
      order by id
    ),
    struct(0 as temperature, 8192 as max_output_tokens, 0.0 as top_p, 1 as top_k, TRUE as flatten_json_output)
  );""")

enrich_carbohydrates = BigQueryInsertJobOperator(
    task_id="enrich_carbohydrates_sql",
    configuration={
        "query": {
            "query": enrich_carbohydrates_sql,
            "useLegacySql": False,
        }
    },    location=region,    dag=dag)

# Create prediction formatted table
create_carbohydrates_ai_sql = (f"""create or replace table {ai_dataset_name}.carbohydrates_predictions_formatted as
    select trim(replace(replace(replace(ml_generate_text_llm_result, '```json', ''), '```', ''), '\\n', '')) as ml_generate_text_llm_result
    from {ai_dataset_name}.carbohydrates_predictions_raw
""")

create_carbohydrates_ai = BigQueryInsertJobOperator(
    task_id="create_carbohydrates_ai",
    configuration={
        "query": {
            "query": create_carbohydrates_ai_sql,
            "useLegacySql": False,
        }
    },    location=region,    dag=dag)


# Update carbohydrates
update_carbohydrates_sql = (f"""update {stg_dataset_name}.Snack set carbohydrates =
    (select CAST(JSON_VALUE(ml_generate_text_llm_result, '$.carbohydrates') AS FLOAT64)
    from {ai_dataset_name}.carbohydrates_predictions_formatted
    where CAST(id AS STRING) = json_value(ml_generate_text_llm_result, '$.id'))
    where 1=1""")

update_carbohydrates = BigQueryInsertJobOperator(
    task_id="update_carbohydrates",
    configuration={
        "query": {
            "query": update_carbohydrates_sql,
            "useLegacySql": False,
        }
    },    location=region,    dag=dag)



data_source_sql = (f"""
    update {stg_dataset_name}.Snack set data_source = 'amc_ai' 
    WHERE 
        category IS NOT NULL 
        OR calories IS NOT NULL
        OR total_fat IS NOT NULL
        OR satured_fat IS NOT NULL
        OR trans_fat IS NOT NULL
        OR polyunsat_fat IS NOT NULL
        OR carbohydrates IS NOT NULL
        OR is_healthy IS NOT NULL""")

update_data_source = BigQueryInsertJobOperator(
    task_id="update_data_source",
    configuration={
        "query": {
            "query": data_source_sql,
            "useLegacySql": False,
        }},location=region,dag=dag)



start >> predict_category >> create_category_ai >> update_category
update_category >> enrich_calories >> create_calories_ai >> update_calories
update_category >> enrich_carbohydrates >> create_carbohydrates_ai >> update_carbohydrates >> end
update_calories >> enrich_total_fat >> create_total_fat_ai >> update_total_fat
update_total_fat >> enrich_trans_fat >> create_trans_fat_ai >> update_trans_fat
update_trans_fat >> enrich_satured_fat >> create_satured_fat_ai >> update_satured_fat
update_satured_fat >> enrich_polyunsat_fat >> create_polyunsat_fat_ai >> update_polyunsat_fat
update_polyunsat_fat >> predict_is_healthy >> create_is_healthy_ai >> update_is_healthy >> update_data_source >> end