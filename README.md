# GCP Data Engineering Pipeline

An end-to-end data engineering pipeline demonstrating how to ingest customer data from Google Cloud Storage, orchestrate processing with Apache Airflow, and load and maintain analytical data in Google BigQuery.

## Architecture

```text
CSV Source
    |
    v
Google Cloud Storage
    |
    v
Apache Airflow
    |
    v
BigQuery Staging
    |
    v
Data Deduplication
    |
    v
BigQuery MERGE / Upsert
    |
    v
BigQuery Target Table
    |
    v
Analytics / Reporting
```

For more details, see [`architecture/architecture.md`](architecture/architecture.md).

## Project Overview

This project demonstrates a cloud-native ETL pipeline using Google Cloud Platform and Apache Airflow.

The pipeline:

1. Identifies the latest customer CSV file stored in GCS.
2. Loads the source data into a BigQuery staging table.
3. Removes duplicate customer records.
4. Uses BigQuery `MERGE` to update existing customers and insert new customers.
5. Removes temporary staging tables after processing.

## Technologies

| Technology | Purpose |
|---|---|
| Google Cloud Storage | Source file storage |
| Apache Airflow | Pipeline orchestration and scheduling |
| BigQuery | Data warehouse and analytical storage |
| BigQuery SQL | Deduplication and MERGE processing |
| Python | Pipeline logic |
| Airflow XCom | Passing runtime metadata between tasks |
| Airflow Variables | Environment-specific configuration |

## Pipeline Workflow

### 1. File Discovery

Airflow uses a Python task to search the configured GCS location and identify the most recently updated customer CSV file.

The selected file URI is passed to downstream tasks using Airflow XCom.

### 2. BigQuery Staging

The selected CSV file is loaded into a BigQuery staging table.

The staging table is recreated for each pipeline execution using:

```text
WRITE_TRUNCATE
```

This provides a clean staging area for each run.

### 3. Data Deduplication

Duplicate customer records are removed using a BigQuery window function:

```sql
ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY updated_at DESC
)
```

This keeps the latest record for each customer.

### 4. MERGE / Upsert

The deduplicated records are merged into the target BigQuery table.

Existing customers are updated, while new customers are inserted.

```sql
MERGE INTO target
USING source
ON target.customer_id = source.customer_id
```

The update logic refreshes:

- `customer_name`
- `email`
- `city`
- `updated_at`

New customers are inserted into the target table.

### 5. Cleanup

Temporary staging and deduplication tables are removed after the successful upsert.

This prevents unnecessary temporary objects from accumulating in BigQuery.

## Airflow DAG

The main DAG is:

```text
dags/gcs_to_bigquery.py
```

### Task Flow

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

### DAG Features

- Daily scheduling
- Retry configuration
- GCS integration
- BigQuery integration
- Dynamic source file discovery
- Airflow XCom
- Airflow Variables
- BigQuery staging
- Data deduplication
- MERGE / UPSERT
- Temporary table cleanup

## Sample Dataset

The repository contains a fictional customer dataset:

```text
data/customers.csv
```

Example fields:

| Column | Description |
|---|---|
| customer_id | Unique customer identifier |
| customer_name | Customer name |
| email | Customer email |
| city | Customer city |
| updated_at | Last update date |

The dataset contains fictional data and does not contain company or customer information.

## BigQuery Schema

The target table schema is documented in:

```text
sql/customers_schema.sql
```

Expected target structure:

| Column | BigQuery Type |
|---|---|
| customer_id | INT64 |
| customer_name | STRING |
| email | STRING |
| city | STRING |
| updated_at | DATE |

## SQL

SQL assets are available under:

```text
sql/
```

The SQL demonstrates:

- BigQuery table creation
- Data deduplication
- Window functions
- MERGE / UPSERT
- Temporary table management

## Configuration

The pipeline keeps environment-specific configuration outside the DAG code.

### Airflow Connection

Configure a Google Cloud connection in Airflow:

```text
Connection ID: google_cloud_default
Connection Type: Google Cloud
```

The connection should provide the credentials required for Airflow to access GCS and BigQuery.

### Airflow Variables

Create the following Airflow Variables:

| Variable | Example Value | Purpose |
|---|---|---|
| `gcp_project_id` | `my-gcp-project` | Google Cloud project ID |
| `bq_dataset_id` | `analytics` | BigQuery dataset |
| `gcs_bucket` | `my-data-bucket` | GCS bucket containing source files |

The DAG references these variables at runtime:

```text
gcp_project_id
bq_dataset_id
gcs_bucket
```

### GCS Input Location

Place the source CSV file under:

```text
gs://<gcs_bucket>/incoming/
```

Example:

```text
gs://my-data-bucket/incoming/customers.csv
```

The DAG automatically searches the `incoming/` prefix and selects the most recently updated customer CSV file.

### BigQuery Dataset

Create the target dataset before running the DAG.

The repository contains the target table schema in:

```text
sql/customers_schema.sql
```

Replace the placeholder project and dataset values in that SQL template with your own GCP environment.

> **Security:** Never commit service-account keys, passwords, access tokens, or other credentials to the repository.

## Data Engineering Concepts Demonstrated

- Cloud data ingestion
- ETL pipeline design
- Airflow DAG orchestration
- Google Cloud Storage
- BigQuery
- BigQuery staging
- Data deduplication
- Window functions
- MERGE / UPSERT
- Incremental data processing
- Pipeline retries
- Runtime configuration
- Airflow XCom
- Airflow Variables
- Temporary table management
- Data warehouse loading

## Repository Structure

```text
gcp-data-engineering-pipeline/
|
├── README.md
├── requirements.txt
├── .gitignore
|
├── architecture/
│   ├── .gitkeep
│   └── architecture.md
|
├── src/
│   ├── .gitkeep
│   └── gcs_ingestion.py
|
├── dags/
│   └── gcs_to_bigquery.py
|
├── sql/
│   └── customers_schema.sql
|
└── data/
    └── customers.csv
```

## Project Architecture

The project follows a simple cloud ETL architecture:

```text
                    +----------------------+
                    |   CSV Source Data    |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    | Google Cloud Storage  |
                    |      incoming/       |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |    Apache Airflow     |
                    |                      |
                    | File Discovery       |
                    | Scheduling           |
                    | Orchestration        |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    | BigQuery Staging     |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    | Deduplication        |
                    | ROW_NUMBER()         |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    | BigQuery MERGE       |
                    | Update + Insert      |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    | BigQuery Target      |
                    | customers            |
                    +----------------------+
```

## Error Handling and Reliability

The Airflow DAG is configured with retry behavior:

```python
default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
```

If a task fails, Airflow can retry the task according to the configured retry policy.

The pipeline also validates whether customer CSV files are available in the configured GCS location before processing.

## Security Considerations

No credentials or service-account keys are stored in the repository.

Environment-specific values are managed through Airflow Variables and the Airflow Google Cloud connection.

The project uses fictional sample data for demonstration purposes.

## Portfolio Note

This project is a portfolio implementation inspired by cloud data engineering patterns and experience with GCS, BigQuery, and Airflow.

It uses fictional data and sanitized configuration and does not contain proprietary company code, credentials, or customer information.

## Future Enhancements

Potential extensions include:

- Data quality validation using Great Expectations
- Schema validation before loading
- Cloud Pub/Sub event-driven ingestion
- Incremental file processing
- BigQuery partitioning and clustering
- Monitoring and alerting
- Airflow task-level data quality checks
- CI/CD using GitHub Actions
- Infrastructure as Code using Terraform
- Power BI or Looker dashboard integration
