"""Cloud Run Job entry point.

Ensures the BigQuery dataset + tables exist, then runs a full pricing load.
Exits non-zero on failure so Cloud Run will surface it.
"""

import argparse
import logging
import sys
import time

from app import bq_setup
from app.config import get_settings
from app.services import loader


def _configure_logging() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_job")
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--filter",
        default=None,
        help="OData $filter override (e.g. \"serviceName eq 'Virtual Machines'\")",
    )
    args = parser.parse_args(argv)

    _configure_logging()
    started = time.monotonic()
    settings = get_settings()
    if args.filter is not None:
        settings.azure_optional_filter = args.filter

    print("[1/2] ensure dataset + tables...", flush=True)
    try:
        bq_setup.ensure_dataset_and_tables(settings=settings)
    except Exception as exc:
        print(f"FATAL: setup failed: {exc}", file=sys.stderr, flush=True)
        return 1

    print("[2/2] load...", flush=True)
    try:
        result = loader.run_load(settings=settings, force=args.force)
    except Exception as exc:
        print(f"FATAL: load failed: {exc}", file=sys.stderr, flush=True)
        return 1

    elapsed = time.monotonic() - started
    print(
        f"done. run_id={result.run_id} rows={result.rows_loaded} "
        f"pages={result.page_count} elapsed={elapsed:.1f}s",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
