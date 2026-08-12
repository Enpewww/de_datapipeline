from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum
from datetime import timedelta, datetime
from api.stats_video import get_playlist_id, get_video_ids, extract_video_data, save_to_json
from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality

# Define local timezone
local_tz = pendulum.timezone("Asia/Jakarta")

# Default Args

default_args = {
    "owner": "luthfi_de",
    "depends_on_past": False,
    "email on_failure": False,
    "email_on_retry": False,
    "email": "luthfi_de@engineer.com",
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2026, 8, 6, tzinfo=local_tz),
    # "end_date": datetime(2030, 12, 31, tzinfo=local_tz),
}

staging_schema = "staging"
core_schema = "core"

with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce JSON file from YouTube API",
    schedule="0 9 * * *",
    catchup=False,
    dagrun_timeout=timedelta(minutes=30),
) as dag:

    # Define dag tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extracted_data = extract_video_data(video_ids)
    save_json_data = save_to_json(extracted_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db",
        wait_for_completion=False,
    )

    # Define task dependencies
    playlist_id >> video_ids >> extracted_data >> save_json_data >> trigger_update_db

with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to update database with YouTube API data into staging and core tables",
    schedule=None,
    catchup=False,
) as dag_update:

    # Define dag tasks
    update_staging = staging_table()
    update_core = core_table()

    # Define task dependencies
    update_staging >> update_core

with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="DAG to run data quality checks on the table",
    schedule= "0 10 * * *",
    catchup=False,
) as dag_update:

    # Define dag tasks
    soda_validate_staging= yt_elt_data_quality(staging_schema)
    soda_validate_core= yt_elt_data_quality(core_schema)

    # Define task dependencies
    soda_validate_staging >> soda_validate_core