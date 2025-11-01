from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'imdb_etl_pipeline',
    default_args=default_args,
    schedule=timedelta(days=1),
    start_date=datetime(2025, 6, 1), 
    catchup=False,
    tags=['imdb', 'etl'],
)

extract_tmdb = BashOperator(
    task_id='extract_tmdb_data',
    bash_command='python {{ var.value.project_path }}/dlt/csv_pipeline.py',
    env={
        'POSTGRES_PASSWORD': '1234'
    },
    dag=dag,
)

transform_dbt = BashOperator(
    task_id='transform_dbt',
    bash_command='cd {{ var.value.dbt_project_path }} && dbt run',
    env={
        'DBT_PROFILES_DIR': '{{ var.value.dbt_project_path }}',
        'POSTGRES_PASSWORD': '1234'
    },
    dag=dag,
)

test_dbt = BashOperator(
    task_id='test_dbt',
    bash_command='cd {{ var.value.dbt_project_path }} && dbt test',
    env={
        'DBT_PROFILES_DIR': '{{ var.value.dbt_project_path }}',
        'POSTGRES_PASSWORD': '1234'
    },
    dag=dag,
)

extract_tmdb >> transform_dbt >> test_dbt
