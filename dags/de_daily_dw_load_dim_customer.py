from datetime import datetime, timedelta
import pendulum

from airflow import DAG
from redshift_operator_plugin import RedshiftOperator


# Set timezone to Australia/Melbourne
local_tz = pendulum.timezone("Australia/Melbourne")

default_args = {
    "owner": "(Data Engineering) Daily Update Dim Customer table",
    "start_date": datetime(2021, 2, 7, 8, 15, tzinfo=local_tz),
    "retries": 1,
    "concurrency": 4,
    "retry_delay": timedelta(minutes=5),
    "email": ["datasquad645@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

# List of sql files to run
sql_files = ["dim_customer"]

with DAG(
    dag_id="de_daily_dw_load_dim_customer",
    default_args=default_args,
    schedule_interval="15 06 * * *",
    max_active_runs=1,
    catchup=False,
    tags=["DE"],
) as dag:

    sql_tasks = {
        sql_file: RedshiftOperator(
            task_id=sql_file, sql=f"/sql/{sql_file}.sql", dag=dag,
        )
        for sql_file in sql_files
    }

# Define dependency graph
[sql_tasks["dim_customer"]]
