import logging
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)

SODA_PATH = "/opt/airflow/dags/include/soda"
DATA_SOURCE = "pg_datasource"


def yt_elt_data_quality(schema):
    try:
        task =BashOperator(
            task_id = f"soda_test_{schema}",
            bash_command = f"soda scan -d {DATA_SOURCE} -c {SODA_PATH}/configuration.yml -v SCHEMA={schema} {SODA_PATH}/{schema}_checks.yml",
        )
        return task
    
    except Exception as e:
        logger.error(f"Error running data quality checks for schema '{schema}': {e}")
        raise e