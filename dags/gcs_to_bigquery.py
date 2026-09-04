"""
GCS to BigQuery ETL Pipeline
----------------------------

Portfolio implementation demonstrating:

1. Discovering the latest CSV file in GCS
2. Dynamically identifying the target table
3. Loading data into a BigQuery staging table
4. Deduplicating records
5. Upserting records into the target table
6. Cleaning up the staging table

This implementation uses Airflow Variables for environment-specific
configuration so that credentials and project-specific information
are not hard-coded.
"""

from datetime import datetime, timedelta
import json
import re

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)


# =========================================================
# Configuration
# =========================================================

GCP_CONN_ID = "google_cloud_default"

GCP_PROJECT_ID = "{{ var.value.gcp_project_id }}"
BQ_DATASET_ID = "{{ var.value.bq_dataset_id }}"
GCS_BUCKET = "{{ var.value.gcs_bucket }}"

GCS_PREFIX = "incoming/"


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# =========================================================
# Table key configuration
# =========================================================

KEY_COLUMNS_RAW = Variable.get(
    "table_key_columns",
    default_var="{}",
)

try:
    TABLE_KEY_COLUMNS = json.loads(KEY_COLUMNS_RAW)
except json.JSONDecodeError:
    TABLE_KEY_COLUMNS = {}


DEFAULT_KEY_COLUMN = Variable.get(
    "default_key_column",
    default_var="id",
)


# =========================================================
# Discover latest source file
# =========================================================

def get_latest_file_and_table_name(**kwargs):
    """
    Identify the latest CSV file available in GCS.

    The function determines:

    - Source GCS URI
    - Target BigQuery table
    - Key column used for upsert

    Values are passed to downstream tasks using XCom.
    """

    ti = kwargs["ti"]

    hook = GCSHook(
        gcp_conn_id=GCP_CONN_ID
    )

    storage_client = hook.get_conn()

    bucket = storage_client.bucket(
        GCS_BUCKET
    )

    object_names = hook.list(
        bucket_name=GCS_BUCKET,
        prefix=GCS_PREFIX,
    )

    csv_objects = [
        object_name
        for object_name in object_names
        if object_name.lower().endswith(".csv")
    ]

    if not csv_objects:
        raise ValueError(
            f"No CSV files found in "
            f"gs://{GCS_BUCKET}/{GCS_PREFIX}"
        )

    latest_blob = None

    for object_name in csv_objects:

        blob = bucket.get_blob(
            object_name
        )

        if blob is None:
            continue

        if (
            latest_blob is None
            or (
                blob.updated
                and blob.updated > latest_blob.updated
            )
        ):
            latest_blob = blob

    if latest_blob is None:
        raise ValueError(
            "Unable to determine the latest CSV file."
        )

    file_name = (
        latest_blob.name
        .split("/")[-1]
    )

    base_name = file_name.rsplit(
        ".",
        1,
    )[0]

    # Remove numeric suffixes such as _01 or _100.
    clean_base_name = re.sub(
        r"_[0-9]+$",
        "",
        base_name,
    )

    table_name = (
        clean_base_name
        .lower()
        .replace("-", "_")
        .replace(" ", "_")
    )

    key_column = TABLE_KEY_COLUMNS.get(
        table_name,
        DEFAULT_KEY_COLUMN,
    )

    source_uri = (
        f"gs://{GCS_BUCKET}/{latest_blob.name}"
    )

    # Store values for downstream tasks.
    ti.xcom_push(
        key="source_uri",
        value=source_uri,
    )

    ti.xcom_push(
        key="table_name",
        value=table_name,
    )

    ti.xcom_push(
        key="key_column",
        value=key_column,
    )

    print(
        f"Latest file: {latest_blob.name}"
    )

    print(
        f"Target table: {table_name}"
    )

    print(
        f"Key column: {key_column}"
    )


# =========================================================
# DAG definition
# =========================================================

with DAG(
    dag_id="gcs_to_bigquery",
    description=(
        "GCS to BigQuery ETL pipeline "
        "with staging and upsert processing."
    ),
    start_date=datetime(
        2026,
        1,
        1,
    ),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=[
        "gcp",
        "gcs",
        "bigquery",
        "etl",
    ],
) as dag:

    # -----------------------------------------------------
    # 1. Discover latest source file
    # -----------------------------------------------------

    get_file_info = PythonOperator(
        task_id="get_file_info",
        python_callable=(
            get_latest_file_and_table_name
        ),
    )

    # -----------------------------------------------------
    # 2. Load source file into staging table
    # -----------------------------------------------------

    load_to_staging = BigQueryInsertJobOperator(
        task_id="load_to_staging",
        configuration={
            "load": {
                "sourceUris": [
                    "{{ ti.xcom_pull("
                    "task_ids='get_file_info', "
                    "key='source_uri'"
                    ") }}"
                ],
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BQ_DATASET_ID,
                    "tableId": (
                        "{{ ti.xcom_pull("
                        "task_ids='get_file_info', "
                        "key='table_name'"
                        ") }}_staging"
                    ),
                },
                "sourceFormat": "CSV",
                "autodetect": True,
                "skipLeadingRows": 1,
                "writeDisposition": (
                    "WRITE_TRUNCATE"
                ),
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    # -----------------------------------------------------
    # 3. Remove duplicate records
    # -----------------------------------------------------

    deduplicate_staging = BigQueryInsertJobOperator(
        task_id="deduplicate_staging",
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE
                    `{{ var.value.gcp_project_id }}.
                    {{ var.value.bq_dataset_id }}.
                    {{ ti.xcom_pull(
                        task_ids='get_file_info',
                        key='table_name'
                    ) }}_deduplicated`
                    AS

                    SELECT *
                    FROM
                    `{{ var.value.gcp_project_id }}.
                    {{ var.value.bq_dataset_id }}.
                    {{ ti.xcom_pull(
                        task_ids='get_file_info',
                        key='table_name'
                    ) }}_staging`

                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY
                            {{ ti.xcom_pull(
                                task_ids='get_file_info',
                                key='key_column'
                            ) }}
                        ORDER BY
                            1
                    ) = 1
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    # -----------------------------------------------------
    # 4. Upsert records into target table
    # -----------------------------------------------------

    upsert_to_target = BigQueryInsertJobOperator(
        task_id="upsert_to_target",
        configuration={
            "query": {
                "query": """
                    MERGE INTO
                    `{{ var.value.gcp_project_id }}.
                    {{ var.value.bq_dataset_id }}.
                    {{ ti.xcom_pull(
                        task_ids='get_file_info',
                        key='table_name'
                    ) }}` AS target

                    USING
                    `{{ var.value.gcp_project_id }}.
                    {{ var.value.bq_dataset_id }}.
                    {{ ti.xcom_pull(
                        task_ids='get_file_info',
                        key='table_name'
                    ) }}_deduplicated` AS source

                    ON target.{{ ti.xcom_pull(
                        task_ids='get_file_info',
                        key='key_column'
                    ) }}
                    =
                    source.{{ ti.xcom_pull(
                        task_ids='get_file_info',
                        key='key_column'
                    ) }}

                    WHEN NOT MATCHED THEN
                        INSERT ROW
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    # -----------------------------------------------------
    # 5. Remove temporary tables
    # -----------------------------------------------------

    cleanup_staging = BigQueryInsertJobOperator(
        task_id="cleanup_staging",
        configuration={
            "query": {
                "query": """
                    DROP TABLE IF EXISTS
                    `{{ var.value.gcp_project_id }}.
                    {{ var.value.bq_dataset_id }}.
                    {{ ti.xcom_pull(
                        task_ids='get_file_info',
                        key='table_name'
                    ) }}_staging`;

                    DROP TABLE IF EXISTS
                    `{{ var.value.gcp_project_id }}.
                    {{ var.value.bq_dataset_id }}.
                    {{ ti.xcom_pull(
                        task_ids='get_file_info',
                        key='table_name'
                    ) }}_deduplicated`;
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    # =====================================================
    # Task dependencies
    # =====================================================

    (
        get_file_info
        >> load_to_staging
        >> deduplicate_staging
        >> upsert_to_target
        >> cleanup_staging
    )
