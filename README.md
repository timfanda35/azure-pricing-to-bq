# azure-pricing-to-bq

Daily loader for the [Azure Retail Prices REST API](https://learn.microsoft.com/en-us/rest/api/cost-management/retail-prices/azure-retail-prices), sinking to **BigQuery**. Runs as a Cloud Run Job; consumers query the dataset directly via SQL.

## What lands in BigQuery

| Table | Shape | Who reads it |
|---|---|---|
| `azure_retail_prices` | Latest snapshot. Not partitioned. Clustered by `service_name, arm_region_name`. | **Default for consumers.** Plain `SELECT *` returns today's prices, no dedup gymnastics. |
| `azure_retail_prices_history` | Append-only history. `PARTITION BY ingestion_date`. **`require_partition_filter = TRUE`.** Same clustering. | Time-travel queries (e.g. price changes over time). |
| `pricing_runs` | Audit: one row per loader invocation. | Operations / monitoring. |

The live table is rebuilt at the end of every successful run by a single atomic `CREATE OR REPLACE TABLE` — consumers either see yesterday's snapshot or today's, never a half-loaded mix.

## Local dev

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env   # fill GCP_PROJECT and GCS_STAGING_BUCKET

ruff check .
pytest -q
```

### Filtered smoke against a real GCP project

```bash
gcloud auth application-default login
export GCP_PROJECT=<dev-project>
export BQ_DATASET=azure_pricing_dev
export GCS_STAGING_BUCKET=<dev-project>-azure-pricing-staging
export AZURE_OPTIONAL_FILTER="serviceName eq 'Azure DNS'"

python -m azure_pricing_to_bq setup
python -m azure_pricing_to_bq load
python -m azure_pricing_to_bq runs --limit 5

bq query --use_legacy_sql=false \
  'SELECT COUNT(*), COUNT(DISTINCT arm_region_name) FROM `azure_pricing_dev.azure_retail_prices`'
```

The history table will reject any query without a `WHERE ingestion_date = …` filter — that protects all consumers from accidental full-table scans.

## CLI

```bash
python -m azure_pricing_to_bq setup
python -m azure_pricing_to_bq load [--force] [--filter "serviceName eq 'X'"]
python -m azure_pricing_to_bq runs [--limit N]
```

Same image, no FastAPI service. Loads are scheduled jobs; ad-hoc inspection is the CLI or `bq query`.

## Docker / Cloud Run Job

```bash
docker build -t azure-pricing-to-bq:dev .

# Local smoke (uses your gcloud ADC creds):
docker compose up
```

The image's default `CMD` is `python run_job.py`, which is exactly what Cloud Run Job invokes.

## Deployment (GCP)

1. **Cloud Build → Artifact Registry** — image on push to `main`.
2. **GCS staging bucket** — same region as the BQ dataset. **Add a lifecycle rule to delete objects older than 7 days** so failed-run debris cleans itself up.
3. **Service account** for the Cloud Run Job:
   - `roles/bigquery.dataEditor` on the dataset
   - `roles/bigquery.jobUser` on the project
   - `roles/storage.objectAdmin` on the staging bucket
4. **Cloud Run Job** `azure-pricing-loader-job`:
   - **task timeout 3600s** (default 600s is too short for a full load)
   - parallelism 1, max retries 1
   - `CMD ["python","run_job.py"]`
5. **Cloud Scheduler**: daily 02:00 UTC → Cloud Run Job admin API with OIDC token (scheduler SA needs `roles/run.invoker`).

There is no Cloud Run Service deployment.

## Cross-team access

Granting another team read access:

- `roles/bigquery.dataViewer` on the dataset (or per-table on `azure_retail_prices` only for tighter scope).
- They also need `roles/bigquery.jobUser` in **their own project** to run queries — they pay their own query cost (standard BigQuery billing pattern).

Idiomatic consumer query:

```sql
SELECT *
FROM `proj.azure_pricing.azure_retail_prices`
WHERE service_name = 'Virtual Machines'
  AND arm_region_name = 'eastus';
```

Time-travel query (partition filter required):

```sql
SELECT *
FROM `proj.azure_pricing.azure_retail_prices_history`
WHERE ingestion_date = DATE '2026-05-20'
  AND service_name = 'Virtual Machines';
```

## Configuration

| Var | Default | Purpose |
|---|---|---|
| `GCP_PROJECT` | — | GCP project ID (required) |
| `BQ_DATASET` | `azure_pricing` | dataset name |
| `BQ_LOCATION` | `US` | dataset region; must match staging bucket region |
| `GCS_STAGING_BUCKET` | — | bucket for intermediate JSONL files (required) |
| `GCS_STAGING_PREFIX` | `ingestion/` | object key prefix |
| `AZURE_API_VERSION` | `2023-01-01-preview` | |
| `AZURE_CURRENCY` | `USD` | |
| `AZURE_MAX_RETRIES` | `5` | |
| `AZURE_REQUEST_TIMEOUT_S` | `30` | |
| `AZURE_OPTIONAL_FILTER` | `` | OData `$filter` string |
| `HTTP_PROXY` | `` | proxy for outbound HTTP requests |
| `HTTPS_PROXY` | `` | proxy for outbound HTTPS requests (Azure API uses HTTPS) |
| `NO_PROXY` | `` | comma-separated list of hosts to bypass the proxy |
| `JSONL_BATCH_SIZE` | `10000` | items per uploaded JSONL file |
| `LOG_LEVEL` | `INFO` | |

## Design notes

- **GCS staging + LOAD JOB**: free; streaming inserts cost ~$0.01/200MB and have stricter quotas.
- **Partition decorator + `WRITE_TRUNCATE`**: replaces today's history partition atomically, no cross-run duplicates possible.
- **`CREATE OR REPLACE TABLE` for the live table**: single atomic statement; no rename gymnastics.
- **`require_partition_filter = TRUE` on history**: protects every consumer from accidental full-table scans without anyone having to think about it.
- **JSONL over Parquet**: nested `savingsPlan` array maps cleanly to `NEWLINE_DELIMITED_JSON` + `ARRAY<STRUCT>` schema; no schema-conversion layer.
- **ADC instead of service-account key files**: Cloud Run's identity is the auth.
- **UUID `run_id`**: BigQuery has no auto-increment; UUID also matches the GCS staging-prefix layout.
- **Empty-response safeguard**: if the API returns zero items, the loader refuses to truncate today's partition (`RuntimeError`) and the failure is recorded in `pricing_runs`.
