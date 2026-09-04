"""
GCS ingestion utilities.

Downloads source files from Google Cloud Storage
for downstream data processing.
"""

from google.cloud import storage


def download_from_gcs(
    bucket_name: str,
    source_blob_name: str,
    destination_file_name: str,
) -> None:
    """
    Download a file from a GCS bucket to a local path.
    """

    client = storage.Client()

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)

    print(
        f"Downloaded gs://{bucket_name}/{source_blob_name} "
        f"to {destination_file_name}"
    )
