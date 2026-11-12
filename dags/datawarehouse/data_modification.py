import logging

logger = logging.getLogger(__name__)
table = "yt_api"


def insert_rows(cur, conn, schema, row):
    try:
        if schema == "staging":
            video_id = "video_id"
            cur.execute(
                f"""
                INSERT INTO {schema}.{table}("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Views", "Likes_Count", "Comments_Count")
                VALUES (%(video_id)s, %(title)s, %(published_at)s, %(duration)s, %(view_count)s, %(like_count)s, %(comment_count)s);
                """,
                row,
            )
        else:
            video_id = "Video_ID"
            cur.execute(
                f"""
                INSERT INTO {schema}.{table}("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Type", "Video_Views", "Likes_Count", "Comments_Count")
                VALUES (%(Video_ID)s, %(Video_Title)s, %(Upload_Date)s, %(Duration)s, %(Video_Type)s, %(Video_Views)s, %(Likes_Count)s, %(Comments_Count)s)
                """,
                row,
            )

        conn.commit()
        logger.info(f"Inserted row with Video_ID: {row[video_id]}")

    except Exception as e:
        logger.error(f"Error inserting row with Video_ID: {row[video_id]}")
        raise e


def update_rows(cur, conn, schema, row):
    try:
        if schema == "staging":
            video_id = "video_id"
            upload_date = "published_at"
            video_title = "title"
            video_views = "view_count"
            likes_count = "like_count"
            comments_count = "comment_count"
        else:
            video_id = "Video_ID"
            upload_date = "Upload_Date"
            video_title = "Video_Title"
            video_views = "Video_Views"
            likes_count = "Likes_Count"
            comments_count = "Comments_Count"

        if schema == "staging":
            cur.execute(
                f"""
                UPDATE {schema}.{table}
                SET "Video_Title" = %({video_title})s,
                    "Video_Views" = %({video_views})s, 
                    "Likes_Count" = %({likes_count})s, 
                    "Comments_Count" = %({comments_count})s
                WHERE "Video_ID" = %({video_id})s AND "Upload_Date" = %({upload_date})s;
                """,
                row,
            )
        else:
            cur.execute(
                f"""
                UPDATE {schema}.{table}
                SET "Video_Title" = %({video_title})s,
                    "Video_Views" = %({video_views})s, 
                    "Likes_Count" = %({likes_count})s, 
                    "Comments_Count" = %({comments_count})s,
                    "updated_at" = NOW()
                WHERE "Video_ID" = %({video_id})s AND "Upload_Date" = %({upload_date})s;
                """,
                row,
            )

        conn.commit()
        logger.info(f"Updated row with Video_ID: {row[video_id]}")

    except Exception as e:
        logger.error(f"Error updating row with Video_ID: {row[video_id]} - {e}")
        raise e


def delete_rows(cur, conn, schema, ids_to_delete):
    try:
        if not ids_to_delete:
            return

        ids_list = list(ids_to_delete)

        # 'schema' and 'table' are internal constants, safe for f-string interpolation.
        # 'Video_ID' values originate from external data sources and are parameterized
        # using '= ANY(%s)' with a list tuple to prevent SQL injection.
        cur.execute(
            f'DELETE FROM {schema}.{table} WHERE "Video_ID" = ANY(%s);',
            (ids_list,),
        )

        conn.commit()
        logger.info(f"Deleted rows with Video_IDs: {ids_list}")

    except Exception as e:
        logger.error(f"Error deleting rows with Video_IDs: {ids_to_delete} - {e}")
        raise e


# Developed by Ahmed Rizk Gawish
