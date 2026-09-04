# GCP Data Engineering Pipeline Architecture

This document describes the architecture of the end-to-end GCP data engineering pipeline.

## Architecture Diagram

```text
                    +----------------------+
                    |    CSV Source Data   |
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
                    |  Scheduling          |
                    |  File Discovery      |
                    |  Orchestration       |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |  BigQuery Staging    |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    | Data Deduplication   |
                    |    ROW_NUMBER()      |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |    BigQuery MERGE    |
                    |    Update + Insert   |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |   BigQuery Target    |
                    |      customers       |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    | Analytics / Reporting|
                    +----------------------+
```

## Architecture Components

### 1. CSV Source

The pipeline starts with customer data stored as CSV files.

For this portfolio project, the sample data is fictional and stored in:

```text
data/customers.csv
```

### 2. Google Cloud Storage

Google Cloud Storage acts as the cloud landing zone for incoming CSV files.

The pipeline expects files under:

```text
gs://<bucket-name>/incoming/
```

The GCS bucket is configured through an Airflow Variable rather than being hard-coded.

### 3. Apache Airflow

Apache Airflow orchestrates the complete data pipeline.

The Airflow DAG is responsible for:

- Scheduling the pipeline
- Discovering the latest source file
- Passing runtime metadata using XCom
- Triggering BigQuery processing tasks
- Managing task dependencies
- Retrying failed tasks

The main DAG is:

```text
dags/gcs_to_bigquery.py
```

### 4. BigQuery Staging

The selected CSV file is first loaded into a staging table.

Example:

```text
customers_staging
```

The staging layer provides an intermediate area where the incoming data can be processed before updating the target table.

### 5. Data Deduplication

The staging data is deduplicated using a BigQuery window function.

```sql
ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY updated_at DESC
)
```

This ensures that only the latest record for each customer is retained.

### 6. BigQuery MERGE

The deduplicated data is merged into the target BigQuery table.

The pipeline uses the `customer_id` as the matching key.

```sql
MERGE INTO target
USING source
ON target.customer_id = source.customer_id
```

The operation supports two scenarios:

**Existing customer**

The existing record is updated with the latest customer information.

**New customer**

A new record is inserted into the target table.

### 7. BigQuery Target

The final customer data is stored in the BigQuery target table:

```text
customers
```

Expected columns:

| Column | Type |
|---|---|
| customer_id | INT64 |
| customer_name | STRING |
| email | STRING |
| city | STRING |
| updated_at | DATE |

### 8. Analytics / Reporting

The BigQuery target table can be used as the analytical source for downstream reporting and business analytics.

## Airflow Task Flow

The DAG follows this execution sequence:

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

### Task Responsibilities

| Task | Responsibility |
|---|---|
| `get_file_info` | Finds the latest customer CSV in GCS |
| `load_to_staging` | Loads the CSV into BigQuery staging |
| `deduplicate_staging` | Removes duplicate customer records |
| `upsert_to_target` | Updates existing records and inserts new records |
| `cleanup_staging` | Removes temporary BigQuery tables |

## Data Flow

```text
Source
  |
  | CSV
  v
GCS
  |
  | Latest file
  v
Airflow
  |
  | Orchestration
  v
BigQuery Staging
  |
  | Deduplication
  v
Deduplicated Data
  |
  | MERGE
  v
BigQuery Target
  |
  v
Analytics
```

## Configuration Flow

Environment-specific configuration is separated from the DAG code.

```text
Airflow Variables
        |
        +---- gcp_project_id
        |
        +---- bq_dataset_id
        |
        +---- gcs_bucket
        |
        v
   Airflow DAG
        |
        v
GCS + BigQuery
```

The Airflow Google Cloud connection is configured separately:

```text
google_cloud_default
```

This prevents cloud credentials and environment-specific values from being embedded directly in the source code.

## Design Principles

The project demonstrates the following data engineering principles:

- Separation of source, staging, and target data
- Workflow orchestration
- Cloud object storage
- Data warehouse loading
- Data deduplication
- Incremental update patterns
- MERGE / UPSERT processing
- Runtime configuration
- Retry-based pipeline reliability
- Temporary resource cleanup
- Separation of configuration from application code

## Repository Components

```text
gcp-data-engineering-pipeline/
|
├── README.md
|
├── architecture/
│   ├── .gitkeep
│   └── architecture.md
|
├── dags/
│   └── gcs_to_bigquery.py
|
├── src/
│   └── gcs_ingestion.py
|
├── sql/
│   └── customers_schema.sql
|
└── data/
    └── customers.csv
```

## Future Architecture Enhancements

The architecture can be extended with additional GCP services and production capabilities, such as:

```text
Pub/Sub
   |
   v
Event-driven ingestion
   |
   v
Cloud Storage
   |
   v
Airflow
   |
   v
BigQuery
```

Other potential enhancements include:

- Automated data quality checks
- Schema validation
- BigQuery partitioning
- BigQuery clustering
- Monitoring and alerting
- CI/CD
- Infrastructure as Code
- Event-driven ingestion
