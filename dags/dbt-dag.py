import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

profile_config = ProfileConfig(
    profile_name = "default",
    target_name = "dev",
    profile_mapping = SnowflakeUserPasswordProfileMapping(
        conn_id = "snowflake_conn",
        profile_args = {"database":"ai_adoption","schema": "sariga"},
    )
)

dbt_snowflake_dag = DbtDag(
    project_config = ProjectConfig(DBT_ROOT_PATH / "ai_adoption",),
    operator_args = {"install_deps":True},
    profile_config = profile_config,
    execution_config = ExecutionConfig(dbt_executable_path = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",),
    schedule_interval = "@daily",
    start_date = datetime(2025, 4, 16),
    catchup = False,
    dag_id = "dbt_dag"
)