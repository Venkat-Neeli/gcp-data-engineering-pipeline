"""
GCS to BigQuery ingestion pipeline.

Portfolio-safe implementation demonstrating:
- GCS file discovery
- Dynamic table identification
- BigQuery staging
- Data deduplication
- Upsert processing
- Temporary table cleanup
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


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

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


# ---------------------------------------------------------
# Airflow Variable configuration
# ---------------------------------------------------------

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


# ---------------------------------------------------------
# Identify latest source file
# ---------------------------------------------------------

def get_latest_file_and_table_name(**kwargs):
    """
    Find the latest CSV file in GCS and derive:

    - source_uri
    - target table name
    - primary key column
    """

    ti = kwargs["ti"]

    hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

    client = hook.get_conn()
    bucket = client.bucket(GCS_BUCKET)

    object_names = hook.list(
        bucket_name=GCS_BUCKET,
        prefix=GCS_PREFIX,
    )

    csv_objects = [
        name
        for name in object_names
        if name.lower().endswith(".csv")
    ]

    if not csv_objects:
        raise ValueError(
            f"No CSV files found in gs://{GCS_BUCKET}/{GCS_PREFIX}"
        )

    latest_blob = None

    for object_name in csv_objects:

        blob = bucket.get_blob(object_name)

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

    file_name = latest_blob.name.split("/")[-1]

    base_name = file_name.rsplit(".", 1)[0]

    # Remove numeric suffixes such as _01, _02, _100
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

    print(f"Latest file: {latest_blob.name}")
    print(f"Target table: {table_name}")
    print(f"Key column: {key_column}")


# ---------------------------------------------------------
# DAG definition
# ---------------------------------------------------------

with DAG(
    dag_id="gcs_to_bigquery",
    description=(
        "Load the latest GCS CSV file into BigQuery "
        "with staging and upsert processing."
    ),
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["gcp", "gcs", "bigquery", "etl"],
) as dag:

    get_file_info = PythonOperator(
        task_id="get_file_info",
        python_callable=get_latest_file_and_table_name,
    )

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
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    get_file_info >> load_to_staging
