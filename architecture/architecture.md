# GCP Data Engineering Pipeline Architecture

## Pipeline Flow

```text
CSV Source
    |
    v
Google Cloud Storage
    |
    v
Apache Airflow
    |
    +----------------------+
    |                      |
    v                      v
File Discovery       Pipeline Scheduling
    |
    v
BigQuery Staging
    |
    v
Data Deduplication
    |
    v
BigQuery MERGE
    |
    v
BigQuery Target Table
    |
    v
Analytics / Reporting
