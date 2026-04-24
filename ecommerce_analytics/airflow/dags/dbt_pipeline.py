from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'neeti',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='ecommerce_dbt_pipeline',
    default_args=default_args,
    description='Daily dbt build for ShopNova ecommerce pipeline',
    schedule='0 6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'ecommerce']
) as dag:

    dbt_build = BashOperator(
        task_id='dbt_build',
        bash_command='cd /usr/local/airflow/include/dbt && dbt build --profiles-dir /usr/local/airflow/include'
    )