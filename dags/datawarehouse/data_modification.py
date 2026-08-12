import logging

logger = logging.getLogger(__name__)
table = "yt_api"

def insert_rows(conn, curr, schema, row):

    try:
        if schema == "staging":

            video_id = 'video_id'

            curr.execute(f""" INSERT INTO {schema}.{table} ("Video_Id", "Video_Title", "Upload_Date", "Duration", "Video_Views", "Likes_Count", "Comments_Count")
            VALUES (%(video_id)s, %(title)s, %(published_at)s, %(duration)s, %(views_count)s, %(likes_count)s, %(comments_count)s);
            """, row,
        )

        else:
            video_id = 'Video_Id'

            curr.execute(f""" INSERT INTO {schema}.{table} ("Video_Id", "Video_Title", "Upload_Date", "Duration", "Video_Type", "Video_Views", "Likes_Count", "Comments_Count")
            VALUES (%(Video_Id)s, %(Video_Title)s, %(Upload_Date)s, %(Duration)s, %(Video_Type)s, %(Video_Views)s, %(Likes_Count)s, %(Comments_Count)s)
            """, row,
        )

        conn.commit()
        logger.info(f"Inserted row into {schema}.{table}: {row[video_id]}")

    except Exception as e:
        logger.error(f"Error inserting row into {schema}.{table}: {row[video_id]}")
        raise e

def update_rows(curr, conn, schema, row):

    try:
        # Staging
        if schema == "staging":
            video_id = "video_id"
            upload_date = "published_at"
            video_title = "title"
            video_views = "views_count"
            likes_count = "likes_count"
            comments_count = "comments_count"

            curr.execute(
                f"""
                UPDATE {schema}.{table}
                SET
                    "Video_Title" = %({video_title})s,
                    "Video_Views" = %({video_views})s,
                    "Likes_Count" = %({likes_count})s,
                    "Comments_Count" = %({comments_count})s
                WHERE "Video_Id" = %({video_id})s
                  AND "Upload_Date" = %({upload_date})s;
                """,
                row
            )

        # Core
        else:
            curr.execute(
                f"""
                UPDATE {schema}.{table}
                SET
                    "Video_Title" = %(Video_Title)s,
                    "Duration" = %(Duration)s,
                    "Video_Type" = %(Video_Type)s,
                    "Video_Views" = %(Video_Views)s,
                    "Likes_Count" = %(Likes_Count)s,
                    "Comments_Count" = %(Comments_Count)s
                WHERE "Video_Id" = %(Video_Id)s
                  AND "Upload_Date" = %(Upload_Date)s;
                """,
                row
            )

        conn.commit()

        logger.info(
            f"Updated row in {schema}.{table}: {row[video_id if schema == 'staging' else 'Video_Id']}"
        )

    except Exception as e:
        logger.error(
            f"Error updating row in {schema}.{table}: {e}",
            exc_info=True
        )
        raise e

def delete_rows(curr, conn, schema, ids_to_delete):

    try:

        ids_to_delete = f"""({', '.join(f"'{id}'" for id in ids_to_delete)})"""

        curr.execute(
            f"""
            DELETE FROM {schema}.{table} 
            WHERE "Video_Id" IN {ids_to_delete};
            """
        )

        conn.commit()
        logger.info(f"Deleted rows from {schema}.{table}: {ids_to_delete}")

    except Exception as e:
        logger.error(f"Error deleting rows from {schema}.{table}: {ids_to_delete} - {e}")
        raise e

