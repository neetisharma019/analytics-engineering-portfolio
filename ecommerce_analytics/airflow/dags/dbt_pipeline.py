from airflow import DAG
from airflow.operators.bash import BashOperator
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import DuckDBUserPasswordProfileMapping
from datetime import datetime, timedelta
from pathlib import Path

DBT_PROJECT_PATH = Path('/usr/local/airflow/include/dbt')

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
        target_name='snowflake',
        profiles_yml_filepath=Path('/usr/local/airflow/include/profiles.yml')
    ),
    execution_config=ExecutionConfig(
        dbt_executable_path='/usr/local/bin/dbt'
    ),
    schedule='0 6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['dbt', 'ecommerce', 'cosmos']
)