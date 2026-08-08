from datawarehouse.data_utils import get_conn_cursor, conn_curr_close, create_schema, create_table, get_video_ids
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformation import transform_data

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = "yt_api"

@task
def staging_table():

    schema = 'staging'

    conn, curr = None, None

    try:
        conn, curr = get_conn_cursor()

        YT_data = load_data()

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(curr, schema)

        for row in YT_data:

            if len(table_ids) == 0:
                insert_rows(conn, curr, schema, row)

            else:
                if row['video_id'] in table_ids:
                    update_rows(curr, conn, schema, row)
                else:
                    insert_rows(conn, curr, schema, row)

        ids_json = {row['video_id'] for row in YT_data}

        ids_to_delete = set(table_ids) - ids_json

        if ids_to_delete:
            delete_rows(curr, conn, schema, ids_to_delete)

        logger.info(f"Staging table '{schema}.{table}' updated successfully.")

    except Exception as e:
        logger.error(f"Error updating staging table '{schema}.{table}': {e}")
        raise e

    # Ensure connection and cursor are closed even if an error occurs
    finally:
        if conn and curr:
            conn_curr_close(conn, curr)


@task
def core_table():

    schema = 'core'

    conn, curr = None, None

    try:
        conn, curr = get_conn_cursor()

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(curr, schema)

        current_video_ids = set()

        curr.execute(f"""SELECT * FROM staging.{table};""")
        rows = curr.fetchall()

        for row in rows:

            current_video_ids.add(row['Video_Id'])

            if len(table_ids) == 0:
                transformed_row = transform_data(row)
                insert_rows(conn, curr, schema, transformed_row)

            else:
                transformed_row = transform_data(row)

                if transformed_row['Video_Id'] in table_ids:
                    update_rows(curr, conn, schema, transformed_row)

                else:
                    insert_rows(conn, curr, schema, transformed_row)

        ids_to_delete = set(table_ids) - current_video_ids

        if ids_to_delete:
            delete_rows(curr, conn, schema, ids_to_delete)

        logger.info(f"Core table '{schema}.{table}' updated successfully.")

    except Exception as e:
        logger.error(f"Error updating Core table '{schema}.{table}': {e}")
        raise e

    finally:
        # Ensure connection and cursor are closed even if an error occurs
        if conn and curr:
            conn_curr_close(conn, curr) 