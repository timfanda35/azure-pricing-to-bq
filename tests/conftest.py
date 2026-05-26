import pytest

from app.config import Settings


@pytest.fixture
def settings() -> Settings:
    return Settings(
        gcp_project="test-project",
        bq_dataset="test_dataset",
        bq_location="US",
        gcs_staging_bucket="test-bucket",
        gcs_staging_prefix="ingestion/",
        azure_api_version="2023-01-01-preview",
        azure_currency="USD",
        azure_max_retries=3,
        azure_request_timeout_s=5,
        azure_optional_filter="",
        jsonl_batch_size=100,
    )
