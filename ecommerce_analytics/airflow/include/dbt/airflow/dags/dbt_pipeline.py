from airflow import DAG
from airflow.operators.bash import BashOperator
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import DuckDBUserPasswordProfileMapping
from datetime import datetime, timedelta
from pathlib import Path

DBT_PROJECT_PATH = Path('/Users/neetisharma/Desktop/analytics-engineering-portfolio/ecommerce_analytics')

default_args = {
    'owner': 'neeti',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

ecommerce_dbt_dag = DbtDag(
    dag_id='ecommerce_dbt_cosmos',
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=ProfileConfig(
        profile_name='ecommerce_analytics',
        target_name='dev',
        profiles_yml_filepath=Path('/Users/neetisharma/.dbt/profiles.yml')
    ),
    execution_config=ExecutionConfig(
        dbt_executable_path='/usr/local/bin/dbt'
    ),
    schedule_interval='0 6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['dbt', 'ecommerce', 'cosmos']
)