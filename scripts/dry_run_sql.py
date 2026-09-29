"""Validate every sql/*.sql against BigQuery with the parameters the bot binds.

Run this ON THE SERVER (where the BigQuery credentials and .env live) after
pulling a change to sql/ and before restarting the bot.

    python scripts/dry_run_sql.py             # dry run only: checks SQL, tables,
                                              # columns and params. Bills 0 bytes.
    python scripts/dry_run_sql.py --execute   # also runs each query once and
                                              # prints the row count + GB scanned.

Exit code 0 = every case passed, 1 = at least one failed (details printed).
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "sql"

P = bigquery.ScalarQueryParameter


def build_cases(today: str) -> dict[str, list[tuple[str, list[bigquery.ScalarQueryParameter]]]]:
    """file -> [(label, params)], mirroring what bot/bq_client.py binds."""
    return {
        "apf_function.sql": [
            ("all countries", [P("target_country", "STRING", None)]),
            ("TH", [P("target_country", "STRING", "TH")]),
        ],
        "dpf_function.sql": [
            ("all / all methods", [P("target_country", "STRING", None), P("selected_pgw", "STRING", None)]),
            ("BD / DPP", [P("target_country", "STRING", "BD"), P("selected_pgw", "STRING", "DPP")]),
        ],
        "dpf_yesterday_full_function.sql": [
            ("all / DPP baseline", [P("target_country", "STRING", None), P("selected_pgw", "STRING", "DPP")]),
            ("TH / all methods", [P("target_country", "STRING", "TH"), P("selected_pgw", "STRING", None)]),
        ],
        "dist_function.sql": [
            ("all / today", [P("target_date", "DATE", today), P("selected_country", "STRING", None),
                             P("selected_pgw", "STRING", None)]),
            ("PH / DPP / today", [P("target_date", "DATE", today), P("selected_country", "STRING", "PH"),
                                  P("selected_pgw", "STRING", "DPP")]),
        ],
        "pmh_function.sql": [
            ("TH / today", [P("target_date", "DATE", today), P("selected_country", "STRING", "TH")]),
        ],
        "pmh_week_function.sql": [
            ("TH / as of today", [P("as_of_date", "DATE", today), P("selected_country", "STRING", "TH")]),
        ],
        "usage_function.sql": [
            ("bot identity", []),
        ],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--execute", action="store_true", help="run the queries for real (default: dry run)")
    ap.add_argument("--only", help="run a single file, e.g. apf_function.sql")
    args = ap.parse_args()

    load_dotenv(ROOT / ".env")
    project = os.environ.get("BQ_PROJECT")
    location = os.environ.get("BQ_LOCATION", "US")
    if not project:
        print("BQ_PROJECT is not set (expected the job project, e.g. kz-dp-ops)")
        return 1

    client = bigquery.Client(project=project, location=location)
    print(f"project={project} location={location} mode={'EXECUTE' if args.execute else 'DRY RUN'}\n")

    today = dt.date.today().isoformat()
    failed = 0
    for name, cases in build_cases(today).items():
        if args.only and name != args.only:
            continue
        sql = (SQL_DIR / name).read_text(encoding="utf-8")
        if name == "usage_function.sql":
            sql = sql.replace("__PROJECT_ID__", project).replace("__BQ_LOCATION__", location.lower())
        for label, params in cases:
            cfg = bigquery.QueryJobConfig(
                query_parameters=params,
                dry_run=not args.execute,
                use_query_cache=False,
            )
            try:
                job = client.query(sql, job_config=cfg)
                gb = (job.total_bytes_processed or 0) / 1e9
                if args.execute:
                    rows = list(job.result())
                    cols = ", ".join(rows[0].keys()) if rows else "(no rows)"
                    print(f"OK    {name:36s} {label:22s} rows={len(rows):<6d} {gb:6.3f} GB  cols: {cols}")
                else:
                    print(f"OK    {name:36s} {label:22s} would scan {gb:6.3f} GB")
            except Exception as e:  # noqa: BLE001 - report every failure, keep going
                failed += 1
                msg = str(e).splitlines()[0]
                print(f"FAIL  {name:36s} {label:22s} {msg}")

    print(f"\n{'ALL PASSED' if not failed else f'{failed} FAILED'}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
