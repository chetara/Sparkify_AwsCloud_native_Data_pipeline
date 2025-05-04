from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'chetara',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='sparkify_etl_dag',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['sparkify'],
) as dag:

    run_spark_etl = BashOperator(
        task_id='run_etl',
        bash_command='python /app/spark_etl.py',
        env={
            'AWS_ACCESS_KEY_ID': '{{ var.value.AWS_ACCESS_KEY_ID }}',
            'AWS_SECRET_ACCESS_KEY': '{{ var.value.AWS_SECRET_ACCESS_KEY }}',
            'AWS_REGION': '{{ var.value.AWS_REGION }}'
        }
    )
