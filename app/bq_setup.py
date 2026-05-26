import logging
from pathlib import Path

from google.cloud import bigquery

from app.config import Settings, get_settings

logger = logging.getLogger(__name__)

SCHEMA_DIR = Path(__file__).parent / "bq_schema"
HISTORY_SCHEMA_PATH = SCHEMA_DIR / "azure_retail_prices_history.json"
RUNS_SCHEMA_PATH = SCHEMA_DIR / "pricing_runs.json"

HISTORY_TABLE = "azure_retail_prices_history"
RUNS_TABLE = "pricing_runs"


def _history_table_ddl(project: str, dataset: str, schema_cols_sql: str) -> str:
    """Build the CREATE TABLE IF NOT EXISTS statement for the history table.

    The schema is rendered inline (rather than relying on `schema_from_json`) so
    that PARTITION BY / CLUSTER BY / OPTIONS can be set in the same statement,
    and so the require_partition_filter option is enforced from creation.
    """
    return (
        f"CREATE TABLE IF NOT EXISTS `{project}.{dataset}.{HISTORY_TABLE}` (\n"
        f"{schema_cols_sql}\n"
        f")\n"
        f"PARTITION BY ingestion_date\n"
        f"CLUSTER BY service_name, arm_region_name\n"
        f"OPTIONS(require_partition_filter = TRUE,\n"
        f"        description = 'Azure Retail Prices snapshots, one partition per ingestion_date.')"
    )


def _runs_table_ddl(project: str, dataset: str, schema_cols_sql: str) -> str:
    """Build the CREATE TABLE IF NOT EXISTS statement for the audit table."""
    return (
        f"CREATE TABLE IF NOT EXISTS `{project}.{dataset}.{RUNS_TABLE}` (\n"
        f"{schema_cols_sql}\n"
        f")\n"
        f"OPTIONS(description = 'Audit row per loader invocation.')"
    )


def _schema_field_to_sql(field: bigquery.SchemaField) -> str:
    """Render a SchemaField as the column fragment of a CREATE TABLE DDL."""
    if field.field_type in ("RECORD", "STRUCT"):
        inner = ", ".join(_schema_field_to_sql(f) for f in field.fields)
        base = f"STRUCT<{inner}>"
    else:
        base = field.field_type
    if field.mode == "REPEATED":
        base = f"ARRAY<{base}>"
    not_null = " NOT NULL" if field.mode == "REQUIRED" else ""
    return f"  `{field.name}` {base}{not_null}"


def _schema_cols_sql(schema: list[bigquery.SchemaField]) -> str:
    return ",\n".join(_schema_field_to_sql(f) for f in schema)


def ensure_dataset_and_tables(
    client: bigquery.Client | None = None,
    settings: Settings | None = None,
) -> None:
    """Ensure the dataset and history table exist. Idempotent.

    The live `azure_retail_prices` table is created by the loader's
    `CREATE OR REPLACE TABLE` swap at the end of every successful run, so it
    is not created here.
    """
    settings = settings or get_settings()
    client = client or bigquery.Client(project=settings.gcp_project, location=settings.bq_location)

    dataset_ref = bigquery.Dataset(f"{settings.gcp_project}.{settings.bq_dataset}")
    dataset_ref.location = settings.bq_location
    client.create_dataset(dataset_ref, exists_ok=True)
    logger.info(
        "bq.dataset.ensured project=%s dataset=%s location=%s",
        settings.gcp_project,
        settings.bq_dataset,
        settings.bq_location,
    )

    history_schema = client.schema_from_json(str(HISTORY_SCHEMA_PATH))
    history_ddl = _history_table_ddl(
        settings.gcp_project, settings.bq_dataset, _schema_cols_sql(history_schema)
    )
    client.query(history_ddl).result()
    logger.info(
        "bq.table.ensured table=%s.%s.%s",
        settings.gcp_project,
        settings.bq_dataset,
        HISTORY_TABLE,
    )

    runs_schema = client.schema_from_json(str(RUNS_SCHEMA_PATH))
    runs_ddl = _runs_table_ddl(
        settings.gcp_project, settings.bq_dataset, _schema_cols_sql(runs_schema)
    )
    client.query(runs_ddl).result()
    logger.info(
        "bq.table.ensured table=%s.%s.%s",
        settings.gcp_project,
        settings.bq_dataset,
        RUNS_TABLE,
    )
