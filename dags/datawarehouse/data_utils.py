from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table = "yt_api"

def get_conn_cursor():
    """
    Get a connection and cursor to the Postgres database using Airflow's PostgresHook.
    
    Returns:
        conn: A connection object to the Postgres database.
        cur: A cursor object for executing queries on the database.
    """
    # Create a PostgresHook instance
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    
    # Get a connection from the hook
    conn = hook.get_conn()
    
    # Create a cursor with RealDictCursor to get results as dictionaries
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    return conn, cur

def conn_curr_close(conn, cur):
    """
    Close the cursor and connection to the Postgres database.
    
    Args:
        conn: A connection object to the Postgres database.
        cur: A cursor object for executing queries on the database.
    """
    # Close the cursor
    cur.close()
    
    # Close the connection
    conn.close()

def create_schema(schema):

    conn, cur = get_conn_cursor()

    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"

    cur.execute(schema_sql)
    conn.commit()

    conn_curr_close(conn, cur)

def create_table(schema):

    conn, cur = get_conn_cursor()

    if schema == "staging":
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            "Video_Id" VARCHAR PRIMARY KEY NOT NULL,
            "Video_Title" TEXT NOT NULL,
            "Upload_Date" TIMESTAMP NOT NULL,
            "Duration" VARCHAR NOT NULL,
            "Video_Views" BIGINT,
            "Likes_Count" BIGINT,
            "Comments_Count" BIGINT
        );
        """

    else:
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            "Video_Id" VARCHAR PRIMARY KEY NOT NULL,
            "Video_Title" TEXT NOT NULL,
            "Upload_Date" TIMESTAMP NOT NULL,
            "Duration" TIME NOT NULL,
            "Video_Type" VARCHAR NOT NULL,
            "Video_Views" BIGINT,
            "Likes_Count" BIGINT,
            "Comments_Count" BIGINT
        );
        """

    cur.execute(table_sql)

    conn.commit()

    conn_curr_close(conn, cur)


def get_video_ids(cur, schema):

    cur.execute(f"""SELECT "Video_Id" FROM {schema}.{table};""")
    ids = cur.fetchall()

    video_ids = [row["Video_Id"] for row in ids]

    return video_ids
