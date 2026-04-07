# UPDATE 04 Mar 2026

## Summary
- Added PGW-aware support for deposit commands with `dpp`.
- Kept existing command formats backward compatible.
- Added PGW filtering in SQL using `method LIKE '<pgw>%'`.
- Updated report titles/subtitles to show PGW dynamically (example: `BD DPP Deposit Performance by Country`).
- Fixed DPP estimation baseline to use full local-yesterday completed deposits.
- Kept DPF table logic as time-capped comparison (today, yesterday, yesterday-1 all up to current local time).
- Simplified DPP estimation sentence to a concise one-line format.

## Command Behavior
- `/dpf <country|a>`: unchanged, all methods.
- `/dpf <pgw> <country|a>`: new PGW-aware mode (example: `/dpf dpp BD`).
- `/dist <country|a> <YYYYMMDD>`: unchanged, all methods.
- `/dist <pgw> <country|a> <YYYYMMDD>`: new PGW-aware mode (example: `/dist dpp a 20260401`).

## DPP Estimation Logic
- Realtime % is still derived from the DPF table trend (time-capped data).
- Baseline amount is now full completed volume of local yesterday.
- Estimated today volume = full yesterday volume * (1 + realtime%).
- If full-yesterday baseline is unavailable, estimation message is skipped with a clear unavailable note.

## Message Format
- Current concise output:
- `DPP estimate <DD/MM/YYYY>: ~<EST> (yesterday <DD/MM/YYYY>: <FULL_YDAY>, with <+/-X.XX%> at <HH:MM GMT+offset>).`

## Files Updated
- `main.py`
- `bot/bq_client.py`
- `bot/table_renderer.py`
- `sql/dpf_function.sql`
- `sql/dist_function.sql`
- `sql/dpf_yesterday_full_function.sql`
- `README.md`

## Teammate Sync Notes
- Pull latest branch and restart bot service.
- Verify with:
- `/dpf dpp BD`
- `/dist dpp a <YYYYMMDD>`
- Check that:
- table rows are time-capped across 3 days,
- estimation uses full yesterday baseline,
- title includes `DPP` when PGW is provided.
