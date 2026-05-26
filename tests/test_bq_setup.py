from unittest.mock import MagicMock

from google.cloud import bigquery

from app import bq_setup


def _real_schema(path):
    """Load a real SchemaField list from a JSON file without needing GCP creds."""
    real_client = bigquery.Client.__new__(bigquery.Client)
    return bigquery.Client.schema_from_json(real_client, str(path))


def _mock_bq_client_with_real_schema():
    """A mock BQ client whose `schema_from_json` returns the actual schema files."""
    history_schema = _real_schema(bq_setup.HISTORY_SCHEMA_PATH)
    runs_schema = _real_schema(bq_setup.RUNS_SCHEMA_PATH)

    def _schema_from_json(path):
        if path == str(bq_setup.HISTORY_SCHEMA_PATH):
            return history_schema
        if path == str(bq_setup.RUNS_SCHEMA_PATH):
            return runs_schema
        raise AssertionError(f"unexpected schema path: {path}")

    client = MagicMock()
    client.schema_from_json.side_effect = _schema_from_json
    return client


def test_ensure_dataset_and_tables_creates_dataset_and_both_tables(settings):
    client = _mock_bq_client_with_real_schema()

    bq_setup.ensure_dataset_and_tables(client=client, settings=settings)

    # ---- Dataset ----
    client.create_dataset.assert_called_once()
    ds_arg = client.create_dataset.call_args.args[0]
    assert ds_arg.location == settings.bq_location
    assert client.create_dataset.call_args.kwargs.get("exists_ok") is True

    # ---- Both schemas loaded ----
    schema_paths = [c.args[0] for c in client.schema_from_json.call_args_list]
    assert str(bq_setup.HISTORY_SCHEMA_PATH) in schema_paths
    assert str(bq_setup.RUNS_SCHEMA_PATH) in schema_paths

    # ---- Two DDLs issued ----
    assert client.query.call_count == 2
    ddls = [c.args[0] for c in client.query.call_args_list]

    history_ddl = next(d for d in ddls if "azure_retail_prices_history" in d)
    assert "CREATE TABLE IF NOT EXISTS" in history_ddl
    assert (
        f"`{settings.gcp_project}.{settings.bq_dataset}.azure_retail_prices_history`" in history_ddl
    )
    assert "PARTITION BY ingestion_date" in history_ddl
    assert "CLUSTER BY service_name, arm_region_name" in history_ddl
    assert "require_partition_filter = TRUE" in history_ddl
    # Nested savings_plan must render as ARRAY<STRUCT<...>>
    assert "ARRAY<STRUCT<" in history_ddl
    # Price columns must be BIGNUMERIC, not NUMERIC — BQ NUMERIC's DECIMAL(38, 9)
    # truncates Azure prices like 51.5193995147 (10 fractional digits).
    assert "`unit_price` BIGNUMERIC" in history_ddl
    assert "`retail_price` BIGNUMERIC" in history_ddl
    assert "`tier_minimum_units` BIGNUMERIC" in history_ddl
    # No scalar NUMERIC fields should remain anywhere in the DDL.
    assert " NUMERIC" not in history_ddl.replace("BIGNUMERIC", "")

    runs_ddl = next(d for d in ddls if "pricing_runs" in d)
    assert "CREATE TABLE IF NOT EXISTS" in runs_ddl
    assert f"`{settings.gcp_project}.{settings.bq_dataset}.pricing_runs`" in runs_ddl
    # The audit table is not partitioned and does not require a partition filter.
    assert "PARTITION BY" not in runs_ddl
    assert "require_partition_filter" not in runs_ddl

    # Each query was awaited.
    assert client.query.return_value.result.call_count == 2


def test_ensure_dataset_and_tables_is_idempotent(settings):
    """A second call should issue the same idempotent calls without raising."""
    client = _mock_bq_client_with_real_schema()

    bq_setup.ensure_dataset_and_tables(client=client, settings=settings)
    bq_setup.ensure_dataset_and_tables(client=client, settings=settings)

    # Each call: 1 create_dataset + 2 query (history + runs) = 2 datasets, 4 queries.
    assert client.create_dataset.call_count == 2
    assert client.query.call_count == 4
    for call in client.create_dataset.call_args_list:
        assert call.kwargs.get("exists_ok") is True
    for call in client.query.call_args_list:
        assert "CREATE TABLE IF NOT EXISTS" in call.args[0]
