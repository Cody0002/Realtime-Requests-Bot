-- =====================================================================
-- DPF (Deposit Performance) — KURA VERSION
-- =====================================================================
-- Drop-in replacement for the kz-dp-prod dpf_function.sql, sourced entirely
-- from the Kura data warehouse. Same parameters, same output columns.
--
--   Data project : kz-kura           (location US)
--   Job project  : kz-dp-ops         (bot identity has no jobs.create on kz-kura)
--   Realtime     : kz-kura.prod_dw.fundingTx     (raw landing, ~50 days retention)
--   Brand dim    : kz-kura.int_dw.brand_account  (account_id -> brand/groupName/country/tz)
--
-- Params:
--   @target_country : 2-letter country code (e.g. 'TH'), or NULL for all countries
--   @selected_pgw   : PGW name prefix (e.g. 'dpp'), or NULL for all
--                     'dpp' / 'dumpling' both match Dumpling-style methods
--
-- What changed vs the kz-dp-prod version:
--   * SOURCE 1 realtime  : kz_pg_to_bq_realtime.ext_funding_tx -> prod_dw.fundingTx
--                          (identical column names; same upstream app table)
--   * SOURCE 2 crm_gold  : REMOVED. Kura reads the production DB directly, so the
--                          gap that backfill patched should not exist.
--   * SOURCE 3/4 DPP     : REMOVED. Kura has no dpp_gold tables; DPP is identified
--                          from fundingTx.method / providerKey for EVERY country
--                          (incl. TH/PH). With the DPP filter on, rows collapse to a
--                          single DPP brand/group per country so Avg is a true
--                          average deposit, as the old TH/PH dpp_gold branch did.
--   * country/group/brand: int_dw.brand_account instead of the account table
--                          (LEFT(reqCurrency,2) kept as a fallback).
--   * timezone           : per-brand brand_account.tz (fallback: per-country IANA list).
--   * dedup              : prod_dw is raw landing and may repeat a row per id, so
--                          QUALIFY on id, newest updatedAt wins.
--   * soft deletes       : deletedAt IS NULL (fundingTx is a paranoid model).
--
-- Output: date, country, group, brand, AverageDeposit, TotalDeposit, Weightage
--         (unchanged — main.py / table_renderer.py need no edit)
-- =====================================================================

WITH
-- Constant UTC bound so BigQuery can prune partitions. 3 local days across
-- timezones spanning UTC-6..UTC+8 is at most ~3.6 days, so 4 days is enough.
global_window AS (
  SELECT
    TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 4 DAY) AS lo,
    CURRENT_TIMESTAMP()                                AS hi
),

-- Timezone fallback for accounts missing from brand_account. brand_account
-- INNER JOINs analysis.group_config, so a brand whose group is absent from that
-- allowlist would otherwise be dropped silently.
tz_fallback AS (
  SELECT country, tz
  FROM UNNEST([
    STRUCT('TH' AS country, 'Asia/Bangkok' AS tz),
    STRUCT('PH' AS country, 'Asia/Manila' AS tz),
    STRUCT('ID' AS country, 'Asia/Jakarta' AS tz),
    STRUCT('PK' AS country, 'Asia/Karachi' AS tz),
    STRUCT('BD' AS country, 'Asia/Dhaka' AS tz),
    STRUCT('BR' AS country, 'America/Sao_Paulo' AS tz),
    STRUCT('MX' AS country, 'America/Mexico_City' AS tz),
    STRUCT('IN' AS country, 'Asia/Kolkata' AS tz),
    STRUCT('CO' AS country, 'America/Bogota' AS tz),
    STRUCT('EG' AS country, 'Africa/Cairo' AS tz),
    STRUCT('PE' AS country, 'America/Lima' AS tz)
  ])
),

-- ============================================================
-- Deposits, deduplicated (raw landing can repeat a row per id)
-- ============================================================
funding AS (
  SELECT
    f.id,
    f.accountId,
    f.createdAt,
    f.netAmount,
    f.reqCurrency,
    f.method,
    f.providerKey
  FROM `kz-kura.prod_dw.fundingTx` AS f
  CROSS JOIN global_window gw
  WHERE f.type      = 'deposit'
    AND f.status    = 'completed'          -- timed-out attempts are not money
    AND f.deletedAt IS NULL                -- paranoid model: soft deletes remain
    AND f.netAmount IS NOT NULL
    AND f.insertedAt >= gw.lo              -- pipeline landing time (partition column)
    AND f.insertedAt <  gw.hi
    AND f.createdAt  >= gw.lo              -- business time
    AND f.createdAt  <  gw.hi
    AND (
      @selected_pgw IS NULL
      OR (
        LOWER(@selected_pgw) IN ('dpp', 'dumpling')
        AND (
          REGEXP_CONTAINS(LOWER(COALESCE(f.method, '')),      r'dumpling|dpp')
          OR REGEXP_CONTAINS(LOWER(COALESCE(f.providerKey, '')), r'dumpling|dpp')
        )
      )
      OR LOWER(COALESCE(f.method, '')) LIKE CONCAT(LOWER(@selected_pgw), '%')
    )
  QUALIFY ROW_NUMBER() OVER (PARTITION BY f.id ORDER BY f.updatedAt DESC) = 1
),

-- ============================================================
-- Attach brand / country / group / local timezone
-- ============================================================
labelled AS (
  SELECT
    COALESCE(UPPER(ba.country), UPPER(LEFT(f.reqCurrency, 2)))  AS country,
    IF(LOWER(COALESCE(@selected_pgw, '')) IN ('dpp', 'dumpling'),
       'DPP', COALESCE(UPPER(ba.groupName), 'UNKNOWN'))          AS `group`,
    IF(LOWER(COALESCE(@selected_pgw, '')) IN ('dpp', 'dumpling'),
       'DPP', COALESCE(UPPER(ba.brand), 'UNKNOWN'))              AS brand,
    COALESCE(ba.tz, tzf.tz, 'Asia/Bangkok')                      AS tz,
    CAST(f.netAmount AS FLOAT64)                                 AS netAmount,
    f.createdAt
  FROM funding f
  LEFT JOIN `kz-kura.int_dw.brand_account` ba
    ON ba.account_id = f.accountId
  LEFT JOIN tz_fallback tzf
    ON tzf.country = UPPER(LEFT(f.reqCurrency, 2))
  WHERE @target_country IS NULL
     OR COALESCE(UPPER(ba.country), UPPER(LEFT(f.reqCurrency, 2))) = @target_country
),

-- ============================================================
-- Local clock per row, then the 3-day window capped at "now"
-- ============================================================
localised AS (
  SELECT
    country,
    `group`,
    brand,
    netAmount,
    DATE(DATETIME(createdAt, tz))            AS local_date,
    TIME(DATETIME(createdAt, tz))            AS local_time,
    DATE(DATETIME(CURRENT_TIMESTAMP(), tz))  AS today_date,
    TIME(DATETIME(CURRENT_TIMESTAMP(), tz))  AS now_time
  FROM labelled
),

capped AS (
  SELECT
    local_date AS date,
    country,
    `group`,
    brand,
    netAmount,
    today_date
  FROM localised
  WHERE local_date BETWEEN DATE_SUB(today_date, INTERVAL 2 DAY) AND today_date
    AND local_time < now_time
),

consolidated AS (
  SELECT
    date,
    country,
    `group`,
    brand,
    today_date,
    AVG(netAmount) AS AverageDeposit,
    SUM(netAmount) AS TotalDeposit
  FROM capped
  GROUP BY date, country, `group`, brand, today_date
),

today_total AS (
  SELECT country, `group`, brand, TotalDeposit AS TotalToday
  FROM consolidated
  WHERE date = today_date
)

SELECT
  c.date,
  c.country,
  c.`group`,
  c.brand,
  c.AverageDeposit,
  ROUND(c.TotalDeposit, 0)                               AS TotalDeposit,
  ROUND(c.TotalDeposit / NULLIF(t.TotalToday, 0), 4)     AS Weightage
FROM consolidated c
LEFT JOIN today_total t
  ON  c.country = t.country
 AND  c.`group` = t.`group`
 AND  c.brand   = t.brand
ORDER BY c.date DESC, c.TotalDeposit DESC;
