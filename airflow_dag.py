import airflow
from airflow import DAG

from airflow.utils.dates import days_ago,datetime

import os
from datetime import timedelta
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

profile_config=ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping
        (
        conn_id="tpch_snow_connid",
        # profile_args={"database": "airbnb_elt", "schema": "staging"},
        )
)
execution_config = ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",)
project_config=ProjectConfig(dbt_project_path="/usr/local/tpch_airflow/dags/dbt/tpch_dbt_snowflake",)

my_cosmos_dag = DbtDag(
            project_config=project_config,
            execution_config=execution_config,
            profile_config=profile_config,
            # normal dag parameters
            dag_id="tpch_dbt_dag",
            # task_id='my_cosmos_dag',
            schedule_interval='@daily', 
            start_date=datetime(2024, 1, 1),
            catchup=False,
)

my_cosmos_dag