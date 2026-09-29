# UPDATE 29 Sep 2026 — all SQL moved to the Kura data warehouse

## Summary
- Every query under `sql/` now reads **Kura** instead of `kz-dp-prod`:
  - `kz-kura.prod_dw.fundingTx` (deposits / withdrawals, raw landing, ~50 days retention)
  - `kz-kura.prod_dw.member` (registrations, `/apf` only)
  - `kz-kura.int_dw.brand_account` (account → brand / group / country / timezone)
- Same pattern as the Lark bot (`lark_bq_bot_telegram_migrated`, commit "switch dpf and dist
  queries to Kura data source"), extended to `apf`, `pmh` and `pmh_week`.
- Parameters and output columns of every query are unchanged, so `main.py` and
  `bot/table_renderer.py` did not need edits.
- Jobs must run in the **job project `kz-dp-ops`** with **location `US`** (the bot identity
  has no `jobs.create` on `kz-kura`). `.env` on the server must be updated (see below).

## What changed per file
| File | Change |
|---|---|
| `sql/dpf_function.sql` | Ported from the Lark bot. `crm_gold` backfill and `dpp_gold` TH/PH sources removed; DPP is detected from `method`/`providerKey` (`dpp`/`dumpling`) for every country. Per-brand timezone from `brand_account.tz`. |
| `sql/dpf_yesterday_full_function.sql` | Ported from the Lark bot's `dpf_dpp_estimate_function.sql`, generalised to any `@selected_pgw`, still returns one row per country with `0` when empty. |
| `sql/dist_function.sql` | Ported from the Lark bot, plus the `@selected_pgw` filter this bot supports. Local date now from `createdAt` (in Kura `insertedAt` is landing time). |
| `sql/apf_function.sql` | New Kura version: registrations from `prod_dw.member`, deposits from `prod_dw.fundingTx`, brand/country/tz from `brand_account`. NAR counts distinct member ids. |
| `sql/pmh_function.sql`, `sql/pmh_week_function.sql` | New Kura versions: `fundingTx` + `brand_account`; brand is `UPPER(brand_account.brand)`; `pmh_week` scan is now bounded (was a full-table scan). |
| `sql/usage_function.sql` | Unchanged; the region qualifier is now lower-cased by the client (`region-us`). |
| `bot/bq_client.py` | `sql/` resolved relative to the package (works from any cwd); Kura docstrings; lowercase region for `/usage`. |
| `bot/config.py` | `BQ_LOCATION` defaults to `US`; warns at startup if it is anything else. |
| `.env.example` | New. Documents `BQ_PROJECT=kz-dp-ops`, `BQ_LOCATION=US`. |
| `scripts/dry_run_sql.py` | New. Dry-runs every query with the bot's parameters using the server credentials. |

## Behaviour changes to be aware of
- **`/dpf dpp <country>`** now shows a single `DPP` group/brand per country for *all* countries
  (before: only TH/PH were collapsed; BD/PK/... were split by brand). This makes the Avg column a
  true average deposit, matching the Lark bot.
- **Soft-deleted transactions are excluded** (`deletedAt IS NULL`) everywhere.
- **Brands missing from `int_dw.brand_account`** fall back to `LEFT(reqCurrency, 2)` for
  country and to a fixed IANA timezone list; their group/brand show as `UNKNOWN`. Registrations
  (`/apf` NAR) for such accounts are dropped because there is no timezone to place them in.
- **Retention**: `prod_dw.fundingTx` keeps roughly the last 50 days. `/dist`, `/pmh_*` for older
  dates return "No results" instead of data. Deep history lives in `kz-kura.int_dw.fundingTx`
  (hourly lag) if it is ever needed.
- `/pmh_*` brand values are upper-cased, so more brands should now match `sql/brand_mapping.csv`.

## Deploy on the server (needs the account that can run Kura queries)
```bash
cd <bot directory>
git pull origin main

# 1. Point the bot at the job project / location.  .env is git-ignored, edit it in place:
#    BQ_PROJECT=kz-dp-ops
#    BQ_LOCATION=US

# 2. Validate every query without spending bytes (dry run). This is the check that
#    matters: it catches missing tables/columns, bad params and permission problems.
python scripts/dry_run_sql.py

#    /apf is the only query touching an object the Lark bot never used. If it fails, check:
bq --project_id=kz-dp-ops --location=US show --schema kz-kura:prod_dw.member

# 3. (optional) run each query once for real and eyeball row counts / GB scanned
python scripts/dry_run_sql.py --execute

# 4. Restart the bot the usual way (systemd service / screen session).
```

## Fixes after the first dry run on the server
- **"Cannot query over table 'kz-kura.prod_dw.fundingTx' without a filter over column(s)
  'insertedAt' that can be used for partition elimination"** (seen on `pmh_week`). The
  table requires a partition filter, and BigQuery only accepts one written as a constant
  expression directly in the `WHERE` clause. Taking the bound from a CTE via `CROSS JOIN`
  does not count, especially when that CTE is referenced more than once. Every query now
  writes its `insertedAt` / `createdAt` bounds inline.
- **`/usage`: "Access Denied ... bigquery.jobs.listAll ... JOBS_BY_PROJECT"**. The query now
  reads `INFORMATION_SCHEMA.JOBS_BY_USER`, which only needs `bigquery.jobs.list` and returns
  the same data (the bot identity's own jobs). If it still fails, the identity lacks
  `bigquery.jobs.list` on `kz-dp-ops`. `/usage` is a hidden admin command, so the dry-run
  script reports it as a WARN and it does not block the deploy.

## Fix: /dpf and /apf showed every brand under one group
- The group comes from Kura `int_dw.brand_account.groupName`, which is formatted differently
  from the old `account.group`. The bot's old string replacements only knew `PH96G1`, `PHBLG`,
  `PHK`, `IDK`, `PKK`, so everything else became `KZO`.
- `bot/group_mapping.py` now parses the label the same way the Lark bot does
  (`extract_sub_group` / `normalize_brand` in its `app.py`): strip the country prefix and
  separators, find `96G` / `BLG` / `WDB` / `KZG`, drop the sub-group number. Result:
  `BLG`, `WDB`, `96G`, `KZO`. No CSV is involved.
- Labels that still cannot be parsed are logged once per command
  (`not recognised, shown under KZO`) so they show up in `journalctl`.
- Check on the server before restarting:
  `python scripts/show_group_mapping.py TH` prints every TH brand with its raw `groupName`
  and the group the bot will show.

## Smoke test in Telegram after restart
- `/dpf a`, `/dpf BD`, `/dpf dpp BD`, `/dpf dpp TH`
- `/dist a <today YYYYMMDD>`, `/dist dpp PH <today>`
- `/apf a`, `/apf TH`
- `/pmh_total TH <yesterday>`, `/pmh_provider a <yesterday>`, `/pmh_week a <today>`
- `/usage` (admin) — should now report the `kz-dp-ops` / `region-us` job history

Check that: tables render with the same columns as before; DPP totals appear for every country
(not only TH/PH); group names still normalise to `96G` / `BLG` / `WDB` / `KZO`.

## Rollback
```bash
git revert <this commit>   # restores the kz-dp-prod queries
# and put BQ_PROJECT / BQ_LOCATION in .env back to the previous values, then restart.
```
