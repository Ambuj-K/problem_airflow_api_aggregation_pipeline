from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from tasks import api_to_db_file
from tasks import db_aggregate_file
# from tasks import db_aggregate_file_spark

# other packages
from datetime import datetime
from datetime import timedelta

# default args for DAG creation
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': '@hourly',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

# DAG creation
dag = DAG(
    dag_id='dag_api_call_aggregate',
    description='Download api data to postgres and aggregate',
    default_args=default_args)

# create python operator to create pipeline for the api call to db insertion
api_db = PythonOperator(
    task_id='api_to_db',
    python_callable=api_to_db_file.api_to_db,
    dag=dag)

# create python operator to create pipeline for the api db to agg db insertion
aggregate_db = PythonOperator(
    task_id='aggregate_db_fill',
    python_callable=db_aggregate_file.api_db_aggregate_fill,
    dag=dag)

api_db >> aggregate_db

# Future scope (spark pipeline)

# pyspark_app_home = Variable.get("PYSPARK_APP_HOME")

# spark_api_db_dump_aggregation = SparkSubmitOperator(
# task_id='api_db_dump_aggregation', conn_id='spark_local', application=f'{
# pyspark_app_home}/spark/db_aggregate_file.py', total_executor_cores=4,
# packages="io.delta:delta-core_2.12:0.7.0,
# org.postgresql:postgresql:42.2.9", executor_cores=2,
