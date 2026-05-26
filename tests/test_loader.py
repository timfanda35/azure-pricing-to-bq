from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from google.cloud import bigquery

from app.services import loader


def _make_item(meter_id: str, *, with_savings_plan: bool = False) -> dict:
    item = {
        "currencyCode": "USD",
        "tierMinimumUnits": 0.0,
        "retailPrice": 0.0024,
        "unitPrice": 0.0024,
        "armRegionName": "eastus",
        "location": "US East",
        "effectiveStartDate": "2022-09-01T00:00:00Z",
        "meterId": meter_id,
        "meterName": "DNS Zone",
        "productId": "DZH318Z0BQ4F",
        "skuId": "DZH318Z0BQ4F/000A",
        "productName": "Azure DNS",
        "skuName": "Standard",
        "serviceName": "Azure DNS",
        "serviceId": "DZH317F1HKN0",
        "serviceFamily": "Networking",
        "unitOfMeasure": "1 Million",
        "type": "Consumption",
        "armSkuName": "",
        "reservationTerm": "",
        "isPrimaryMeterRegion": True,
    }
    if with_savings_plan:
        item["savingsPlan"] = [
            {"unitPrice": 0.001, "retailPrice": 0.002, "term": "1 Year"},
            {"unitPrice": 0.0005, "retailPrice": 0.002, "term": "3 Years"},
        ]
    return item


def _mock_bq_client():
    client = MagicMock(spec=bigquery.Client)
    load_job = MagicMock()
    load_job.errors = None
    load_job.job_id = "test-load-job"
    load_job.output_rows = 0
    client.load_table_from_uri.return_value = load_job
    client.query.return_value.result.return_value = None
    # Real schema (small enough to load in tests)
    real_client = bigquery.Client.__new__(bigquery.Client)
    client.schema_from_json.return_value = bigquery.Client.schema_from_json(
        real_client, str(loader.HISTORY_SCHEMA_PATH)
    )
    return client, load_job


def _mock_gcs_client():
    """Returns (client_mock, captured_uploads) where captured_uploads is a list of (blob_name, items)."""
    captured: list[tuple[str, list[dict]]] = []
    deleted_prefixes: list[str] = []

    client = MagicMock()

    def _bucket(name):
        bucket = MagicMock()
        bucket.name = name

        def _blob(blob_name):
            blob = MagicMock()
            blob.name = blob_name
            return blob

        bucket.blob.side_effect = _blob
        return bucket

    client.bucket.side_effect = _bucket
    # When loader calls gcs_client.list_blobs(bucket, prefix=...), we record + return nothing.
    client.list_blobs.side_effect = lambda bucket, prefix=None: (
        deleted_prefixes.append(prefix) or iter([])
    )
    return client, captured, deleted_prefixes


def test_run_load_happy_path_two_pages(settings):
    settings.jsonl_batch_size = 2  # force 2 batches across 3 items
    bq_client, load_job = _mock_bq_client()
    gcs_client, _captured, deleted_prefixes = _mock_gcs_client()

    pages = [
        (0, [_make_item("m1"), _make_item("m2", with_savings_plan=True)]),
        (1, [_make_item("m3")]),
    ]

    with (
        patch.object(loader.azure_client, "fetch_pages", return_value=iter(pages)),
        patch.object(loader, "upload_jsonl") as upload_mock,
    ):
        result = loader.run_load(settings=settings, bq_client=bq_client, gcs_client=gcs_client)

    # ---- JSONL uploads ----
    assert upload_mock.call_count == 2, "expected two JSONL batch uploads"
    first_call = upload_mock.call_args_list[0]
    second_call = upload_mock.call_args_list[1]
    # Blob names follow page-NNNNN.jsonl pattern under the run_id prefix.
    assert first_call.args[2].endswith("page-00000.jsonl")
    assert second_call.args[2].endswith("page-00001.jsonl")
    assert first_call.args[1] == settings.gcs_staging_bucket
    # First batch: 2 items.
    first_items = first_call.args[3]
    assert len(first_items) == 2
    assert first_items[0]["meter_id"] == "m1"
    assert first_items[1]["meter_id"] == "m2"
    # camelCase -> snake_case happened
    assert first_items[0]["service_name"] == "Azure DNS"
    assert first_items[0]["arm_region_name"] == "eastus"
    # ingestion_date / ingested_at added
    assert first_items[0]["ingestion_date"] == result.run_date.isoformat()
    assert "ingested_at" in first_items[0]
    # savings_plan nested transform
    sp = first_items[1]["savings_plan"]
    assert sp == [
        {"unit_price": 0.001, "retail_price": 0.002, "term": "1 Year"},
        {"unit_price": 0.0005, "retail_price": 0.002, "term": "3 Years"},
    ]
    # Items without savingsPlan get an empty list
    assert first_items[0]["savings_plan"] == []

    # ---- LOAD JOB ----
    bq_client.load_table_from_uri.assert_called_once()
    args, kwargs = bq_client.load_table_from_uri.call_args
    source_uri, destination = args[0], args[1]
    assert source_uri.startswith(f"gs://{settings.gcs_staging_bucket}/")
    assert source_uri.endswith("/*.jsonl")
    assert result.run_id in source_uri
    assert destination.startswith(
        f"{settings.gcp_project}.{settings.bq_dataset}.azure_retail_prices_history$"
    )
    # Partition decorator is YYYYMMDD
    assert destination.endswith(result.run_date.strftime("%Y%m%d"))
    cfg: bigquery.LoadJobConfig = kwargs["job_config"]
    assert cfg.source_format == bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    assert cfg.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
    assert cfg.schema, "schema must be set explicitly"
    load_job.result.assert_called_once()

    # ---- Swap query (find it among the query() calls, which now also include audit DML) ----
    swap_calls = [
        c for c in bq_client.query.call_args_list if "CREATE OR REPLACE TABLE" in c.args[0]
    ]
    assert len(swap_calls) == 1, "exactly one CREATE OR REPLACE TABLE statement expected"
    swap_sql = swap_calls[0].args[0]
    assert "CLUSTER BY service_name, arm_region_name" in swap_sql
    assert "SELECT * EXCEPT(ingestion_date)" in swap_sql
    assert "azure_retail_prices_history" in swap_sql
    swap_cfg: bigquery.QueryJobConfig = swap_calls[0].kwargs["job_config"]
    params = {p.name: p for p in swap_cfg.query_parameters}
    assert "run_date" in params
    assert params["run_date"].type_ == "DATE"
    assert params["run_date"].value == result.run_date

    # ---- Audit hooks fired (start_run INSERT + finish_run UPDATE) ----
    audit_sqls = [c.args[0] for c in bq_client.query.call_args_list]
    assert any("INSERT INTO" in s and "'running'" in s for s in audit_sqls)
    assert any("UPDATE" in s and "'succeeded'" in s for s in audit_sqls)

    # ---- Staging deleted on success ----
    assert len(deleted_prefixes) == 1
    assert result.run_id in deleted_prefixes[0]

    # ---- Result ----
    assert result.rows_loaded == 3
    assert result.page_count == 2


def test_run_load_load_job_error_leaves_staging_in_place(settings):
    bq_client, load_job = _mock_bq_client()
    load_job.errors = [{"reason": "invalid", "message": "boom"}]
    gcs_client, _captured, deleted_prefixes = _mock_gcs_client()

    pages = [(0, [_make_item("m1")])]
    with (
        patch.object(loader.azure_client, "fetch_pages", return_value=iter(pages)),
        patch.object(loader, "upload_jsonl"),
    ):
        with pytest.raises(RuntimeError, match="BigQuery load job failed"):
            loader.run_load(settings=settings, bq_client=bq_client, gcs_client=gcs_client)

    # No swap query attempted (start_run + fail_run are the only audit queries)
    audit_sqls = [c.args[0] for c in bq_client.query.call_args_list]
    assert not any("CREATE OR REPLACE TABLE" in s for s in audit_sqls)
    assert any("INSERT INTO" in s and "'running'" in s for s in audit_sqls)
    assert any("UPDATE" in s and "'failed'" in s for s in audit_sqls)
    # No staging deletion
    assert deleted_prefixes == []


def test_run_load_empty_response_refuses_to_truncate(settings):
    bq_client, _ = _mock_bq_client()
    gcs_client, _captured, deleted_prefixes = _mock_gcs_client()

    pages = [(0, [])]
    with (
        patch.object(loader.azure_client, "fetch_pages", return_value=iter(pages)),
        patch.object(loader, "upload_jsonl"),
    ):
        with pytest.raises(RuntimeError, match="zero items"):
            loader.run_load(settings=settings, bq_client=bq_client, gcs_client=gcs_client)

    bq_client.load_table_from_uri.assert_not_called()
    # No swap, but start_run + fail_run still fired on the audit table.
    audit_sqls = [c.args[0] for c in bq_client.query.call_args_list]
    assert not any("CREATE OR REPLACE TABLE" in s for s in audit_sqls)
    assert any("INSERT INTO" in s and "'running'" in s for s in audit_sqls)
    assert any("UPDATE" in s and "'failed'" in s for s in audit_sqls)
    assert deleted_prefixes == []


def test_run_load_requires_gcp_project_and_bucket(settings):
    settings.gcp_project = ""
    with pytest.raises(ValueError, match="GCP_PROJECT"):
        loader.run_load(settings=settings, bq_client=MagicMock(), gcs_client=MagicMock())

    settings.gcp_project = "p"
    settings.gcs_staging_bucket = ""
    with pytest.raises(ValueError, match="GCS_STAGING_BUCKET"):
        loader.run_load(settings=settings, bq_client=MagicMock(), gcs_client=MagicMock())


def test_transform_item_drops_unknown_fields():
    now = datetime(2026, 5, 26, 12, 0, tzinfo=UTC)
    item = _make_item("m1")
    item["someFutureField"] = "ignore me"
    row = loader._transform_item(item, "2026-05-26", now.isoformat())
    assert "someFutureField" not in row
    assert row["meter_id"] == "m1"
    assert row["ingestion_date"] == "2026-05-26"
