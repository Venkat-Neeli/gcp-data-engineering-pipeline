"""
GCS to BigQuery Customer Pipeline

Demonstrates an Airflow ETL pipeline that:

1. Finds the latest customer CSV in GCS
2. Loads the file into a BigQuery staging table
3. Deduplicates customer records
4. Upserts records into the target table
5. Removes temporary staging tables

All environment-specific values are supplied through
Airflow Variables.
"""

from datetime import datetime, timedelta

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
TABLE_NAME = "customers"
STAGING_TABLE = "customers_staging"
DEDUP_TABLE = "customers_deduplicated"


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# =========================================================
# Discover latest customer file
# =========================================================

def get_latest_customer_file(**kwargs):
    """
    Find the most recently updated customer CSV in GCS
    and pass its URI to downstream tasks through XCom.
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
        and "customer" in object_name.lower()
    ]

    if not csv_objects:
        raise ValueError(
            f"No customer CSV files found in "
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
            "Unable to determine the latest customer file."
        )

    source_uri = (
        f"gs://{GCS_BUCKET}/{latest_blob.name}"
    )

    ti.xcom_push(
        key="source_uri",
        value=source_uri,
    )

    print(
        f"Selected source file: {source_uri}"
    )


# =========================================================
# DAG definition
# =========================================================

with DAG(
    dag_id="gcs_to_bigquery_customers",
    description=(
        "Load customer data from GCS into BigQuery "
        "using staging, deduplication and MERGE."
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
        "airflow",
        "etl",
    ],
) as dag:

    # -----------------------------------------------------
    # 1. Find latest source file
    # -----------------------------------------------------

    get_file_info = PythonOperator(
        task_id="get_file_info",
        python_callable=get_latest_customer_file,
    )

    # -----------------------------------------------------
    # 2. Load GCS file into staging table
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
                    "tableId": STAGING_TABLE,
                },
                "sourceFormat": "CSV",
                "autodetect": True,
                "skipLeadingRows": 1,
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    # -----------------------------------------------------
    # 3. Deduplicate staging data
    # -----------------------------------------------------

    deduplicate_staging = BigQueryInsertJobOperator(
        task_id="deduplicate_staging",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE
                    `{{{{ var.value.gcp_project_id }}}}.
                    {{{{ var.value.bq_dataset_id }}}}.
                    {DEDUP_TABLE}` AS

                    SELECT
                        customer_id,
                        customer_name,
                        email,
                        city,
                        updated_at

                    FROM
                    `{{{{ var.value.gcp_project_id }}}}.
                    {{{{ var.value.bq_dataset_id }}}}.
                    {STAGING_TABLE}`

                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY customer_id
                        ORDER BY updated_at DESC
                    ) = 1
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    # -----------------------------------------------------
    # 4. Upsert into target table
    # -----------------------------------------------------

    upsert_to_target = BigQueryInsertJobOperator(
        task_id="upsert_to_target",
        configuration={
            "query": {
                "query": f"""
                    MERGE INTO
                    `{{{{ var.value.gcp_project_id }}}}.
                    {{{{ var.value.bq_dataset_id }}}}.
                    {TABLE_NAME}` AS target

                    USING
                    `{{{{ var.value.gcp_project_id }}}}.
                    {{{{ var.value.bq_dataset_id }}}}.
                    {DEDUP_TABLE}` AS source

                    ON target.customer_id =
                       source.customer_id

                    WHEN MATCHED THEN
                        UPDATE SET
                            customer_name =
                                source.customer_name,
                            email =
                                source.email,
                            city =
                                source.city,
                            updated_at =
                                source.updated_at

                    WHEN NOT MATCHED THEN
                        INSERT (
                            customer_id,
                            customer_name,
                            email,
                            city,
                            updated_at
                        )
                        VALUES (
                            source.customer_id,
                            source.customer_name,
                            source.email,
                            source.city,
                            source.updated_at
                        )
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    # -----------------------------------------------------
    # 5. Cleanup temporary tables
    # -----------------------------------------------------

    cleanup_staging = BigQueryInsertJobOperator(
        task_id="cleanup_staging",
        configuration={
            "query": {
                "query": f"""
                    DROP TABLE IF EXISTS
                    `{{{{ var.value.gcp_project_id }}}}.
                    {{{{ var.value.bq_dataset_id }}}}.
                    {STAGING_TABLE}`;

                    DROP TABLE IF EXISTS
                    `{{{{ var.value.gcp_project_id }}}}.
                    {{{{ var.value.bq_dataset_id }}}}.
                    {DEDUP_TABLE}`;
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
