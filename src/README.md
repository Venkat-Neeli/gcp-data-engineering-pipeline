# Source Code

This directory contains reusable Python utilities used by the GCP data engineering pipeline.

## `gcs_ingestion.py`

Provides utilities for downloading files from Google Cloud Storage.

### Main Function

```python
download_from_gcs(
    bucket_name,
    source_blob_name,
    destination_file_name,
)
```

The function:

1. Creates a Google Cloud Storage client.
2. Connects to the configured bucket.
3. Identifies the requested GCS object.
4. Downloads the object to a local destination.
5. Logs the download operation.

## Design

The project separates reusable cloud utilities from Airflow orchestration logic.

```text
src/
    |
    └── gcs_ingestion.py
            |
            v
    Google Cloud Storage
```

The Airflow DAG under `dags/` is responsible for orchestration, while reusable Python functionality is kept under `src/`.

## Security

The source code does not contain:

- Service-account credentials
- Passwords
- API keys
- Access tokens
- Environment-specific secrets

Cloud authentication should be configured through the Google Cloud/Airflow connection.
