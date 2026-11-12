from datawarehouse.data_utils import (
    get_conn_cursor,
    close_conn_cursor,
    create_schema,
    create_table,
    get_video_ids,
)
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformation import transform_data

import logging
from datetime import date
from pathlib import Path
from airflow.decorators import task

logger = logging.getLogger(__name__)
project_root = Path(__file__).parents[2]
table = "yt_api"


@task
def staging_table(file_path: str = None, ds: str = None, **context):
    schema = "staging"
    conn, cur = None, None

    # Resolve input path: explicit argument, cross-DAG trigger conf, or execution date (ds)
    if not file_path:
        dag_run = context.get("dag_run")
        if dag_run and dag_run.conf and "file_path" in dag_run.conf:
            file_path = dag_run.conf["file_path"]
        elif ds:
            file_path = str(project_root / f"data/YT_data_{ds}.json")
        else:
            file_path = str(project_root / f"data/YT_data_{date.today()}.json")

    try:
        conn, cur = get_conn_cursor()

        YT_data = load_data(file_path)

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cur, schema)

        for row in YT_data:
            if len(table_ids) == 0:
                insert_rows(cur, conn, schema, row)
            else:
                if row["video_id"] in table_ids:
                    update_rows(cur, conn, schema, row)
                else:
                    insert_rows(cur, conn, schema, row)

        ids_in_json = {row["video_id"] for row in YT_data}
        ids_to_delete = set(table_ids) - ids_in_json

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"{schema} table update completed")

    except Exception as e:
        logger.error(f"An error occurred during the update of {schema} table: {e}")
        raise e

    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)


@task
def core_table():
    schema = "core"
    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cur, schema)
        current_video_ids = set()

        cur.execute(f"SELECT * FROM staging.{table};")
        rows = cur.fetchall()

        for row in rows:
            current_video_ids.add(row["Video_ID"])

            if len(table_ids) == 0:
                transformed_row = transform_data(row)
                insert_rows(cur, conn, schema, transformed_row)
            else:
                transformed_row = transform_data(row)
                if transformed_row["Video_ID"] in table_ids:
                    update_rows(cur, conn, schema, transformed_row)
                else:
                    insert_rows(cur, conn, schema, transformed_row)

        ids_to_delete = set(table_ids) - current_video_ids

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"{schema} table update completed")

    except Exception as e:
        logger.error(f"An error occurred during the update of {schema} table: {e}")
        raise e

    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)


# Developed by Ahmed Rizk Gawish