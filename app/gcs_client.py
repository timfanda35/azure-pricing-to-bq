import io
import json
import logging
from collections.abc import Iterable

from google.cloud import storage

from app.config import Settings, get_settings

logger = logging.getLogger(__name__)


def get_gcs_client(settings: Settings | None = None) -> storage.Client:
    """Build a GCS client bound to the configured project.

    Patch point for tests: `app.gcs_client.storage.Client`.
    """
    settings = settings or get_settings()
    return storage.Client(project=settings.gcp_project or None)


def upload_jsonl(
    client: storage.Client,
    bucket_name: str,
    blob_name: str,
    items: Iterable[dict],
) -> str:
    """Serialize an iterable of dicts as NDJSON and upload to GCS.

    Returns the gs:// URI of the uploaded blob.
    """
    buffer = io.BytesIO()
    count = 0
    for item in items:
        buffer.write(json.dumps(item, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
        buffer.write(b"\n")
        count += 1
    buffer.seek(0)

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_file(buffer, rewind=False, content_type="application/x-ndjson")
    uri = f"gs://{bucket_name}/{blob_name}"
    logger.info("gcs.upload.complete uri=%s rows=%d", uri, count)
    return uri


def delete_prefix(client: storage.Client, bucket_name: str, prefix: str) -> int:
    """Delete every blob under a prefix. Returns the number of blobs deleted."""
    bucket = client.bucket(bucket_name)
    deleted = 0
    for blob in client.list_blobs(bucket, prefix=prefix):
        blob.delete()
        deleted += 1
    logger.info("gcs.prefix.deleted bucket=%s prefix=%s deleted=%d", bucket_name, prefix, deleted)
    return deleted
