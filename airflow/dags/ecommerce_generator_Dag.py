from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator

GENERATOR_SCRIPT_PATH = "/opt/airflow/data_generation/generator.py"
GENERATOR_BURST_SECONDS = 90

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="ecommerce_generator_to_bronze",
    description="Generate ecommerce events and write them directly into Databricks Bronze",
    default_args=default_args,
    schedule=timedelta(minutes=5),
    start_date=datetime(2026,1,1),
    catchup=False,
    tags =["ecommerce","databricks"],
    template_searchpath="/opt/airflow/sql"
) as dag:

    run_generator = BashOperator(
        task_id = "run_generator",
        bash_command=(
            f"python {GENERATOR_SCRIPT_PATH} "
            f"--sink databricks "
            f"--databricks-server-hostname {{{{ var.value.databricks_server_hostname }}}} "
            f"--databricks-http-path {{{{ var.value.databricks_http_path }}}} "
            f"--sessions-per-min 60 "
            f"--duration {GENERATOR_BURST_SECONDS}"
        )
    )

    merge_to_silver = DatabricksSqlOperator(
        task_id="merge_to_silver",
        databricks_conn_id="databricks_id",
        sql="silver.sql"
    )

    run_generator >> merge_to_silver