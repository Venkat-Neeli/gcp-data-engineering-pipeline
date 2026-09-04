# GCP Data Engineering Pipeline

An end-to-end data engineering pipeline demonstrating how to ingest
customer data from Google Cloud Storage, orchestrate processing with
Apache Airflow, and load and maintain analytical data in Google BigQuery.

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
