
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


DBT = "/opt/airflow/dbt_venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/zomato"
DBT_PROFILES = "/home/airflow/.dbt"
with DAG(
    dag_id="zomato_batch",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["zomato", "dbt", "snowflake"],
    doc_md=__doc__,
) as dag:

    reload_raw = BashOperator(
        task_id="reload_raw",
        bash_command=(
            "python /opt/airflow/scripts/load_minio_to_snowflake.py --all"
        ),
    )

    dbt_build_core = BashOperator(
        task_id="dbt_build_core",
        bash_command=(
            f"{DBT} build "
            f"--project-dir {DBT_PROJECT} "
            f"--profiles-dir {DBT_PROFILES}"
        ),
    )

    reload_raw >> dbt_build_core

