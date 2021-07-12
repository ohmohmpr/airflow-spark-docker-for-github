import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

default_args = {"owner": "airflow"}

spark_config = {
    'master':'spark://spark:7077'
}
# create_pet_table, populate_pet_table, get_all_pets, and get_birth_date are examples of tasks created by
# instantiating the Postgres Operator

with DAG(
    dag_id="spark_operator_dag",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
) as dag:
    submit_job = SparkSubmitOperator(
        application="/opt/workspace/spark.py", 
        task_id="submit_job",
        verbose=1,
        conn_id='spark_default',
        name="submit_job",
        conf=spark_config,
        executor_memory="2G",
        jars='local:///opt/workspace/jars/aws-java-sdk-bundle-1.11.271.jar,local:///opt/workspace/jars/spark-sas7bdat-3.0.0-s_2.12.jar,local:///opt/workspace/jars/parso-2.0.11.jar,local:///opt/workspace/jars/hadoop-aws-3.1.2.jar'
    )