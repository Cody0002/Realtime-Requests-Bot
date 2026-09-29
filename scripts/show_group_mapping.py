"""Show how every Kura brand is placed into the report groups (96G / BLG / WDB / KZO).

Run ON THE SERVER, where the BigQuery credentials and .env live:

    python scripts/show_group_mapping.py          # all countries
    python scripts/show_group_mapping.py TH       # one country

Reads kz-kura.int_dw.brand_account (a small dimension table) and applies the same
rule the bot uses in /apf and /dpf (bot/group_mapping.py) to its groupName column.
Columns: country, raw groupName, brand, the group the bot will show, and whether
the groupName was recognised (unrecognised values are shown under KZO).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from bot.group_mapping import resolve_group  # noqa: E402

SQL = """
SELECT DISTINCT
  UPPER(country)   AS country,
  groupName        AS raw_group,
  UPPER(brand)     AS brand
FROM `kz-kura.int_dw.brand_account`
WHERE @country IS NULL OR UPPER(country) = @country
ORDER BY country, raw_group, brand
"""


def main() -> int:
    load_dotenv(ROOT / ".env")
    project = os.environ.get("BQ_PROJECT")
    location = os.environ.get("BQ_LOCATION", "US")
    if not project:
        print("BQ_PROJECT is not set (expected the job project, e.g. kz-dp-ops)")
        return 1
    country = sys.argv[1].strip().upper() if len(sys.argv) > 1 else None

    client = bigquery.Client(project=project, location=location)
    cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("country", "STRING", country)])
    rows = [dict(r) for r in client.query(SQL, job_config=cfg).result()]
    if not rows:
        print("No rows in kz-kura.int_dw.brand_account" + (f" for {country}" if country else ""))
        return 1

    header = f"{'CTRY':4s}  {'RAW groupName':20s}  {'BRAND':14s}  {'SHOWN':5s}  RECOGNISED"
    print(header)
    print("-" * len(header))
    counts: dict[tuple[str, str], int] = {}
    for r in rows:
        shown, recognised = resolve_group(r["country"], r["raw_group"])
        print(f"{str(r['country']):4s}  {str(r['raw_group']):20.20s}  {str(r['brand']):14.14s}  "
              f"{shown:5s}  {'yes' if recognised else 'NO'}")
        counts[(str(r["country"]), shown)] = counts.get((str(r["country"]), shown), 0) + 1

    print("\nBrands per country and group:")
    for (c, g), n in sorted(counts.items()):
        print(f"  {c:4s} {g:5s} {n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
