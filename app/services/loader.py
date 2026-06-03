import logging
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from uuid import uuid4

from google.cloud import bigquery, storage

from app.bq_client import get_bq_client
from app.bq_setup import HISTORY_SCHEMA_PATH, HISTORY_TABLE
from app.config import Settings, get_settings
from app.gcs_client import delete_prefix, get_gcs_client, upload_jsonl
from app.services import azure_client
from app.services import runs as runs_service

logger = logging.getLogger(__name__)

LIVE_TABLE = "azure_retail_prices"


@dataclass
class LoadResult:
    run_id: str
    run_date: date
    rows_loaded: int
    page_count: int
    elapsed_s: float


# Mapping from Azure API camelCase fields to our snake_case BQ columns.
# Fields not in this map are dropped (defensive against future API additions).
_FIELD_MAP: dict[str, str] = {
    "currencyCode": "currency_code",
    "tierMinimumUnits": "tier_minimum_units",
    "retailPrice": "retail_price",
    "unitPrice": "unit_price",
    "armRegionName": "arm_region_name",
    "location": "location",
    "effectiveStartDate": "effective_start_date",
    "meterId": "meter_id",
    "meterName": "meter_name",
    "productId": "product_id",
    "skuId": "sku_id",
    "productName": "product_name",
    "skuName": "sku_name",
    "serviceName": "service_name",
    "serviceId": "service_id",
    "serviceFamily": "service_family",
    "unitOfMeasure": "unit_of_measure",
    "type": "type",
    "armSkuName": "arm_sku_name",
    "reservationTerm": "reservation_term",
    "isPrimaryMeterRegion": "is_primary_meter_region",
}

_SAVINGS_PLAN_FIELD_MAP: dict[str, str] = {
    "unitPrice": "unit_price",
    "retailPrice": "retail_price",
    "term": "term",
}


def _transform_savings_plan(raw: list[dict] | None) -> list[dict]:
    if not raw:
        return []
    out: list[dict] = []
    for sp in raw:
        row = {dst: sp.get(src) for src, dst in _SAVINGS_PLAN_FIELD_MAP.items()}
        out.append(row)
    return out


def _transform_item(item: dict, ingestion_date_str: str, ingested_at_str: str) -> dict:
    row: dict = {dst: item.get(src) for src, dst in _FIELD_MAP.items()}
    row["savings_plan"] = _transform_savings_plan(item.get("savingsPlan"))
    row["ingestion_date"] = ingestion_date_str
    row["ingested_at"] = ingested_at_str
    return row


def run_load(
    settings: Settings | None = None,
    *,
    force: bool = False,
    filter: str | None = None,
    bq_client: bigquery.Client | None = None,
    gcs_client: storage.Client | None = None,
) -> LoadResult:
    """Full pricing load: Azure API -> GCS NDJSON -> BQ LOAD JOB -> swap live table."""
    del (
        force
    )  # reserved for Phase 3 audit-table semantics ("re-run today's load even if succeeded")
    settings = settings or get_settings()
    if filter is not None:
        settings.azure_optional_filter = filter

    if not settings.gcp_project:
        raise ValueError("GCP_PROJECT is required")
    if not settings.gcs_staging_bucket:
        raise ValueError("GCS_STAGING_BUCKET is required")

    bq_client = bq_client or get_bq_client(settings)
    gcs_client = gcs_client or get_gcs_client(settings)

    run_id = uuid4().hex
    now = datetime.now(UTC)
    run_date = now.date()
    ingestion_date_str = run_date.isoformat()
    ingested_at_str = now.isoformat()
    started = time.monotonic()

    staging_prefix = (
        f"{settings.gcs_staging_prefix.rstrip('/')}/{run_id}/"
        if settings.gcs_staging_prefix
        else f"{run_id}/"
    )

    logger.info(
        "loader.start run_id=%s run_date=%s bucket=%s prefix=%s filter=%r",
        run_id,
        run_date,
        settings.gcs_staging_bucket,
        staging_prefix,
        settings.azure_optional_filter or None,
    )

    # ---- 0) Audit: record the run as 'running' ----
    runs_service.start_run(
        bq_client,
        settings,
        run_id=run_id,
        currency=settings.azure_currency,
        api_version=settings.azure_api_version,
        ingestion_date=run_date,
        started_at=now,
    )

    # ---- 1) Stream pages, buffer, upload JSONL batches to GCS ----
    buffer: list[dict] = []
    batch_idx = 0
    rows_loaded = 0
    page_count = 0

    def _flush_buffer() -> None:
        nonlocal buffer, batch_idx, rows_loaded
        if not buffer:
            return
        blob_name = f"{staging_prefix}page-{batch_idx:05d}.jsonl"
        upload_jsonl(gcs_client, settings.gcs_staging_bucket, blob_name, buffer)
        rows_loaded += len(buffer)
        batch_idx += 1
        buffer = []

    try:
        for page_idx, items in azure_client.fetch_all_pages(settings):
            page_count = page_idx + 1
            for item in items:
                buffer.append(_transform_item(item, ingestion_date_str, ingested_at_str))
                if len(buffer) >= settings.jsonl_batch_size:
                    _flush_buffer()
        _flush_buffer()

        if rows_loaded == 0:
            raise RuntimeError(
                "Azure API returned zero items; refusing to truncate today's partition."
            )

        # ---- 2) Submit one LOAD JOB to the history partition decorator ----
        partition_decorator = run_date.strftime("%Y%m%d")
        destination = (
            f"{settings.gcp_project}.{settings.bq_dataset}.{HISTORY_TABLE}${partition_decorator}"
        )
        source_uri = f"gs://{settings.gcs_staging_bucket}/{staging_prefix}*.jsonl"

        schema = bq_client.schema_from_json(str(HISTORY_SCHEMA_PATH))
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=schema,
        )
        load_job = bq_client.load_table_from_uri(source_uri, destination, job_config=job_config)
        logger.info(
            "bq.load_job.submitted run_id=%s job_id=%s source=%s dest=%s",
            run_id,
            load_job.job_id,
            source_uri,
            destination,
        )
        load_job.result(timeout=3300)
        if load_job.errors:
            raise RuntimeError(f"BigQuery load job failed: {load_job.errors}")
        logger.info(
            "bq.load_job.complete run_id=%s job_id=%s output_rows=%s",
            run_id,
            load_job.job_id,
            getattr(load_job, "output_rows", None),
        )

        # ---- 3) Atomic swap of live consumer table ----
        swap_sql = (
            f"CREATE OR REPLACE TABLE `{settings.gcp_project}.{settings.bq_dataset}.{LIVE_TABLE}`\n"
            f"CLUSTER BY service_name, arm_region_name\n"
            f"AS SELECT * EXCEPT(ingestion_date)\n"
            f"   FROM `{settings.gcp_project}.{settings.bq_dataset}.{HISTORY_TABLE}`\n"
            f"   WHERE ingestion_date = @run_date"
        )
        swap_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("run_date", "DATE", run_date)]
        )
        bq_client.query(swap_sql, job_config=swap_config).result()
        logger.info("bq.live_table.swapped run_id=%s table=%s", run_id, LIVE_TABLE)

        # ---- 4) Clean up staging on success ----
        delete_prefix(gcs_client, settings.gcs_staging_bucket, staging_prefix)

        # ---- 5) Audit: mark run as succeeded ----
        runs_service.finish_run(
            bq_client,
            settings,
            run_id=run_id,
            rows_loaded=rows_loaded,
            page_count=page_count,
        )

    except Exception as exc:
        # Leave GCS staging in place for inspection; bucket lifecycle rule cleans up after 7d.
        # The live table is untouched until step 3, so partial failures don't corrupt reads.
        logger.exception("loader.failed run_id=%s", run_id)
        try:
            runs_service.fail_run(bq_client, settings, run_id=run_id, error=str(exc))
        except Exception:
            logger.exception("loader.fail_run.also_failed run_id=%s", run_id)
        raise

    elapsed = time.monotonic() - started
    logger.info(
        "loader.complete run_id=%s rows=%d pages=%d elapsed=%.1fs",
        run_id,
        rows_loaded,
        page_count,
        elapsed,
    )
    return LoadResult(
        run_id=run_id,
        run_date=run_date,
        rows_loaded=rows_loaded,
        page_count=page_count,
        elapsed_s=elapsed,
    )
