import argparse
import logging
import sys

from app import bq_setup
from app.bq_client import get_bq_client
from app.config import get_settings
from app.services import loader
from app.services import runs as runs_service


def _configure_logging() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _cmd_setup(args: argparse.Namespace) -> int:
    del args
    try:
        bq_setup.ensure_dataset_and_tables()
    except Exception as exc:
        print(f"setup failed: {exc}", file=sys.stderr)
        return 1
    print("setup ok")
    return 0


def _cmd_load(args: argparse.Namespace) -> int:
    settings = get_settings()
    try:
        result = loader.run_load(settings=settings, force=args.force, filter=args.filter)
    except Exception as exc:
        print(f"load failed: {exc}", file=sys.stderr)
        return 1
    print(
        f"run_id={result.run_id} run_date={result.run_date} "
        f"rows={result.rows_loaded} pages={result.page_count} "
        f"elapsed={result.elapsed_s:.1f}s"
    )
    return 0


def _cmd_runs(args: argparse.Namespace) -> int:
    settings = get_settings()
    client = get_bq_client(settings)
    try:
        rows = runs_service.list_runs(client, settings, limit=args.limit)
    except Exception as exc:
        print(f"runs failed: {exc}", file=sys.stderr)
        return 1
    if not rows:
        print("(no runs)")
        return 0
    header = f"{'run_id':<34}  {'status':<10}  {'rows':>10}  {'pages':>6}  started_at"
    print(header)
    print("-" * len(header))
    for r in rows:
        print(
            f"{r.run_id:<34}  {r.status:<10}  "
            f"{(r.rows_loaded or 0):>10}  {(r.page_count or 0):>6}  "
            f"{r.started_at.isoformat()}"
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    _configure_logging()
    parser = argparse.ArgumentParser(prog="azure-pricing-to-bq")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_setup = sub.add_parser("setup", help="ensure BigQuery dataset and tables exist")
    p_setup.set_defaults(func=_cmd_setup)

    p_load = sub.add_parser("load", help="run a full (or filtered) pricing load")
    p_load.add_argument("--force", action="store_true")
    p_load.add_argument(
        "--filter",
        default=None,
        help="OData $filter, e.g. \"serviceName eq 'Virtual Machines'\"",
    )
    p_load.set_defaults(func=_cmd_load)

    p_runs = sub.add_parser("runs", help="list recent pricing_runs rows")
    p_runs.add_argument("--limit", type=int, default=10)
    p_runs.set_defaults(func=_cmd_runs)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
