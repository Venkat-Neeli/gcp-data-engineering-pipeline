# Setup Guide

This guide explains the high-level steps required to run the GCS to BigQuery pipeline.

## Prerequisites

The following components are required:

- Google Cloud Platform account
- Google Cloud Storage
- BigQuery
- Apache Airflow
- Python 3.x
- Google Cloud Airflow provider

## 1. Create a GCS Bucket

Create a Google Cloud Storage bucket and an `incoming` folder:

```text
gs://<your-bucket>/incoming/
```

Upload the sample file:

```text
data/customers.csv
```

as:

```text
gs://<your-bucket>/incoming/customers.csv
```

## 2. Create a BigQuery Dataset

Create a BigQuery dataset for the pipeline.

For example:

```text
analytics
```

## 3. Create the Target Table

Use the SQL template provided in:

```text
sql/customers_schema.sql
```

Replace:

```text
project_id.analytics
```

with your own GCP project and dataset.

## 4. Configure Airflow

Configure the Google Cloud connection:

```text
Connection ID:
google_cloud_default
```

The connection should have permission to access:

- Google Cloud Storage
- BigQuery

## 5. Configure Airflow Variables

Create these Airflow Variables:

| Variable | Description |
|---|---|
| `gcp_project_id` | Google Cloud project ID |
| `bq_dataset_id` | BigQuery dataset ID |
| `gcs_bucket` | GCS bucket name |

Example:

```text
gcp_project_id = my-gcp-project
bq_dataset_id = analytics
gcs_bucket = my-data-bucket
```

## 6. Install Dependencies

Install the dependencies listed in:

```text
requirements.txt
```

The main dependencies are:

```text
apache-airflow
google-cloud-storage
google-cloud-bigquery
google-cloud-pubsub
pandas
```

## 7. Deploy the DAG

Copy the DAG into the Airflow DAG directory:

```text
dags/gcs_to_bigquery.py
```

Airflow should automatically discover the DAG.

The DAG ID is:

```text
gcs_to_bigquery_customers
```

## 8. Run the Pipeline

The expected task sequence is:

```text
get_file_info
      |
      v
load_to_staging
      |
      v
deduplicate_staging
      |
      v
upsert_to_target
      |
      v
cleanup_staging
```

The pipeline can be scheduled daily or triggered manually from the Airflow UI.

## 9. Validate the Result

After successful execution, verify the target BigQuery table:

```text
<project>.<dataset>.customers
```

The target table should contain:

- Unique `customer_id` values
- Latest customer information
- New records inserted
- Existing records updated

## Security

Do not store credentials directly in the repository.

Use:

- Airflow Connections
- Airflow Variables
- Google Cloud IAM
- Secret management solutions

Never commit:

```text
*.json
*.key
*.pem
.env
```

containing credentials or secrets.

## Portfolio Environment

This repository uses fictional customer data for demonstration.

It does not contain:

- Company source code
- Production datasets
- Customer information
- Service-account credentials
- Proprietary configuration
