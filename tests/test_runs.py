from datetime import UTC, date, datetime
from unittest.mock import MagicMock

import pytest
from google.cloud import bigquery

from app.services import runs as runs_service


def _mock_client(rows=None):
    client = MagicMock(spec=bigquery.Client)
    job = MagicMock()
    job.result.return_value = iter(rows or [])
    client.query.return_value = job
    return client, job


def test_start_run_inserts_running_row(settings):
    client, job = _mock_client()
    started = datetime(2026, 5, 26, 12, 0, tzinfo=UTC)
    runs_service.start_run(
        client,
        settings,
        run_id="abc123",
        currency="USD",
        api_version="2023-01-01-preview",
        ingestion_date=date(2026, 5, 26),
        started_at=started,
    )

    client.query.assert_called_once()
    sql = client.query.call_args.args[0]
    assert "INSERT INTO" in sql
    assert "pricing_runs" in sql
    assert "'running'" in sql

    cfg: bigquery.QueryJobConfig = client.query.call_args.kwargs["job_config"]
    params = {p.name: p for p in cfg.query_parameters}
    assert params["run_id"].value == "abc123"
    assert params["run_id"].type_ == "STRING"
    assert params["currency"].value == "USD"
    assert params["api_version"].value == "2023-01-01-preview"
    assert params["ingestion_date"].type_ == "DATE"
    assert params["ingestion_date"].value == date(2026, 5, 26)
    assert params["started_at"].type_ == "TIMESTAMP"
    assert params["started_at"].value == started
    job.result.assert_called_once()


def test_finish_run_sets_succeeded(settings):
    client, job = _mock_client()
    runs_service.finish_run(client, settings, run_id="abc123", rows_loaded=42, page_count=3)

    sql = client.query.call_args.args[0]
    assert "UPDATE" in sql
    assert "'succeeded'" in sql
    assert "WHERE run_id = @run_id" in sql
    params = {p.name: p for p in client.query.call_args.kwargs["job_config"].query_parameters}
    assert params["rows_loaded"].value == 42
    assert params["rows_loaded"].type_ == "INT64"
    assert params["page_count"].value == 3
    assert params["run_id"].value == "abc123"
    job.result.assert_called_once()


def test_fail_run_truncates_long_errors(settings):
    client, _job = _mock_client()
    long_error = "x" * 10000
    runs_service.fail_run(client, settings, run_id="abc123", error=long_error)

    sql = client.query.call_args.args[0]
    assert "UPDATE" in sql
    assert "'failed'" in sql
    params = {p.name: p for p in client.query.call_args.kwargs["job_config"].query_parameters}
    assert len(params["error"].value) == 8000


def test_fail_run_handles_empty_error(settings):
    client, _job = _mock_client()
    runs_service.fail_run(client, settings, run_id="abc123", error="")
    params = {p.name: p for p in client.query.call_args.kwargs["job_config"].query_parameters}
    assert params["error"].value == ""


def test_list_runs_returns_dataclasses(settings):
    rows = [
        {
            "run_id": "r1",
            "started_at": datetime(2026, 5, 26, 12, 0, tzinfo=UTC),
            "finished_at": datetime(2026, 5, 26, 12, 5, tzinfo=UTC),
            "status": "succeeded",
            "rows_loaded": 100,
            "page_count": 2,
            "currency": "USD",
            "api_version": "2023-01-01-preview",
            "ingestion_date": date(2026, 5, 26),
            "error": None,
        },
        {
            "run_id": "r2",
            "started_at": datetime(2026, 5, 25, 12, 0, tzinfo=UTC),
            "finished_at": None,
            "status": "failed",
            "rows_loaded": None,
            "page_count": None,
            "currency": "USD",
            "api_version": "2023-01-01-preview",
            "ingestion_date": date(2026, 5, 25),
            "error": "boom",
        },
    ]
    client, _job = _mock_client(rows=rows)

    result = runs_service.list_runs(client, settings, limit=5)

    sql = client.query.call_args.args[0]
    assert "SELECT" in sql
    assert "ORDER BY started_at DESC" in sql
    params = {p.name: p for p in client.query.call_args.kwargs["job_config"].query_parameters}
    assert params["limit"].value == 5

    assert len(result) == 2
    assert result[0].run_id == "r1"
    assert result[0].status == "succeeded"
    assert result[0].rows_loaded == 100
    assert result[1].status == "failed"
    assert result[1].error == "boom"


def test_get_run_returns_none_when_missing(settings):
    client, _job = _mock_client(rows=[])
    result = runs_service.get_run(client, settings, run_id="missing")
    assert result is None


def test_get_run_returns_dataclass_when_present(settings):
    rows = [
        {
            "run_id": "r1",
            "started_at": datetime(2026, 5, 26, 12, 0, tzinfo=UTC),
            "finished_at": datetime(2026, 5, 26, 12, 5, tzinfo=UTC),
            "status": "succeeded",
            "rows_loaded": 100,
            "page_count": 2,
            "currency": "USD",
            "api_version": "2023-01-01-preview",
            "ingestion_date": date(2026, 5, 26),
            "error": None,
        },
    ]
    client, _job = _mock_client(rows=rows)
    result = runs_service.get_run(client, settings, run_id="r1")
    assert result is not None
    assert result.run_id == "r1"


def test_module_does_not_open_a_real_bq_client_at_import():
    """Importing runs.py must not need GCP creds."""
    # The import already happened at module load; this assertion just documents intent.
    assert hasattr(runs_service, "PricingRun")
    with pytest.raises(TypeError):
        # PricingRun requires all kwargs — proves the dataclass is intact.
        runs_service.PricingRun()
