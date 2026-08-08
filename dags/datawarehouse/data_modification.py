import logging

logger = logging.getLogger(__name__)
table = "yt_api"

def insert_rows(conn, curr, schema, row):

    try:
        if schema == "staging":

            video_id = 'video_id'

            curr.execute(f""" INSERT INTO {schema}.{table} ("Video_Id", "Video_Title", "Upload_Date", "Duration", "Video_Views", "Likes_Count", "Comments_Count")
            VALUES (%(video_id)s, %(title)s, %(published_at)s, %(duration)s, %(view_count)s, %(like_count)s, %(comment_count)s);
            """, row,
        )

        else:
            video_id = 'Video_id'

            curr.execute(f""" INSERT INTO {schema}.{table} ("Video_Id", "Video_Title", "Upload_Date", "Duration", "Video_Type", "Video_Views", "Likes_Count", "Comments_Count")
            VALUES (%(Video_Id)s, %(Video_Title)s, %(Upload_Date)s, %(Duration)s, %(Video_Type)s, %(Video_Views)s, %(Like_Count)s, %(Comment_Count)s)
            """, row,
        )

        conn.commit()
        logger.info(f"Inserted row into {schema}.{table}: {row[video_id]}")

    except Exception as e:
        logger.error(f"Error inserting row into {schema}.{table}: {row[video_id]}")
        raise e

def update_rows(curr, conn, schema, row):

    try:
        # Stagging
        if schema == "staging":
            video_id = 'video_id'
            upload_date = 'published_at'
            video_title = 'title'
            video_views = 'view_count'
            likes_count = 'like_count'
            comments_count = 'comment_count'

        # Core
        else:
            video_id = 'Video_id'
            upload_date = 'Upload_Date'
            video_title = 'Video_Title'
            video_views = 'Video_Views'
            likes_count = 'Like_Count'
            comments_count = 'Comment_Count'

        curr.execute(
            f"""
            UPDATE {schema}.{table} 
            SET "Video_Title" = %({video_title})s,
                "Video_Views" = %({video_views})s,
                "Likes_Count" = %({likes_count})s, 
                "Comments_Count" = %({comments_count})s 
            WHERE "Video_Id" = %({video_id})s AND "Upload_Date" = %({upload_date})s;
            """, row

        )

        conn.commit()

        logger.info(f"Updated row in {schema}.{table}: {row[video_id]}")

    except Exception as e:
        logger.error(f"Error updating row in {schema}.{table}: {row[video_id]} - {e}")
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

