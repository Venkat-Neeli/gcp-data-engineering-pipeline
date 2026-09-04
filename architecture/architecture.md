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

Components
Component	Purpose
Google Cloud Storage	Stores incoming CSV files
Apache Airflow	Orchestrates and schedules the pipeline
BigQuery	Provides staging and analytical storage
BigQuery SQL	Performs deduplication and MERGE/upsert
Airflow XCom	Passes runtime metadata between tasks
Airflow Variables	Stores environment-specific configuration
Processing Pattern

The pipeline follows a staged ETL approach:

Discover the latest source file.
Load the source file into BigQuery staging.
Remove duplicate records.
Merge new and existing records into the target table.
Remove temporary staging tables.
