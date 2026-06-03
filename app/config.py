from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    gcp_project: str = ""
    bq_dataset: str = "azure_pricing"
    bq_location: str = "US"
    gcs_staging_bucket: str = ""
    gcs_staging_prefix: str = "ingestion/"

    azure_api_version: str = "2023-01-01-preview"
    azure_currency: str = "USD"
    azure_max_retries: int = 5
    azure_request_timeout_s: int = 30
    azure_optional_filter: str = ""
    # CSV override for the serviceFamily partition list. Empty = use the built-in
    # list from app.services.azure_client.SERVICE_FAMILIES.
    azure_service_families: str = ""

    http_proxy: str = ""
    https_proxy: str = ""
    no_proxy: str = ""

    log_level: str = "INFO"

    jsonl_batch_size: int = Field(default=10000, ge=100, le=200000)


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
