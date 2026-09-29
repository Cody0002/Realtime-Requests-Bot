-- =====================================================================
-- PMH WEEK (Mon -> as-of day, vs same span last week) — KURA VERSION
-- =====================================================================
-- Drop-in replacement for the kz-dp-prod pmh_week_function.sql. Same params,
-- same output columns (brand -> group_name mapping stays in Python).
--
--   Data project : kz-kura           (location US)
--   Job project  : kz-dp-ops
--   Realtime     : kz-kura.prod_dw.fundingTx
--   Brand dim    : kz-kura.int_dw.brand_account
--
-- Params:
--   @as_of_date       DATE    the YYYY-MM-DD passed with the command
--   @selected_country STRING  2-letter country code, or NULL for all
--
-- ⚠ RETENTION: prod_dw.fundingTx keeps roughly the last 50 days, so an
-- as-of date older than ~5 weeks returns no rows.
--
-- What changed vs the kz-dp-prod version:
--   * source       : ext_funding_tx + account -> prod_dw.fundingTx + int_dw.brand_account
--   * brand        : brand_account.brand (UPPER) instead of account.name
--   * country / tz : brand_account (fallback: LEFT(reqCurrency,2) + IANA list)
--   * scan window  : the old query scanned the whole table; now bounded to
--                    [prev_start - 1d, as_of + 2d] on createdAt with an insertedAt
--                    lower bound for partition pruning
--   * soft deletes : deletedAt IS NULL
--   * dedup        : QUALIFY on id, newest updatedAt wins
--
-- Output: period, tnx_type, providerKey, method, brand, status, country,
--         avg_diff_seconds_transaction, total_count,
--         transaction_within_180s, transaction_within_300s, transaction_within_900s
-- =====================================================================

WITH
bounds AS (
  SELECT
    DATE_TRUNC(@as_of_date, WEEK(MONDAY))                            AS cur_start,
    @as_of_date                                                      AS cur_end,
    DATE_SUB(DATE_TRUNC(@as_of_date, WEEK(MONDAY)), INTERVAL 7 DAY)  AS prev_start,
    DATE_SUB(@as_of_date, INTERVAL 7 DAY)                            AS prev_end
),

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

tx AS (
  SELECT
    f.id,
    f.type,
    f.status,
    f.accountId,
    f.reqCurrency,
    f.createdAt,
    f.completedAt,
    f.providerKey,
    f.method,
    f.netAmount
  FROM `kz-kura.prod_dw.fundingTx` AS f
  WHERE f.type   IN ('deposit', 'withdraw')
    AND f.status IN ('completed', 'error', 'timeout', 'errors')
    AND f.deletedAt IS NULL
    -- Partition filter (BigQuery requires one on insertedAt). It must be a constant
    -- expression written inline here: a bound taken from a CTE is not used for
    -- partition elimination and the query is rejected.
    -- [prev_start - 1d, as_of + 2d) covers both week spans in any timezone UTC-6..UTC+8.
    AND f.insertedAt >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(DATE_TRUNC(@as_of_date, WEEK(MONDAY)), INTERVAL 7 DAY)), INTERVAL 1 DAY)   -- lower bound only
    AND f.createdAt  >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(DATE_TRUNC(@as_of_date, WEEK(MONDAY)), INTERVAL 7 DAY)), INTERVAL 1 DAY)
    AND f.createdAt  <  TIMESTAMP_ADD(TIMESTAMP(@as_of_date), INTERVAL 2 DAY)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY f.id ORDER BY f.updatedAt DESC) = 1
),

base AS (
  SELECT
    t.type,
    COALESCE(UPPER(ba.country), UPPER(LEFT(t.reqCurrency, 2)))   AS country,
    t.createdAt,
    t.completedAt,
    t.providerKey,
    t.method,
    UPPER(ba.brand)                                               AS brand_name,
    CASE WHEN t.status = 'errors' THEN 'error' ELSE t.status END  AS status,
    t.netAmount,
    DATE(DATETIME(t.createdAt, COALESCE(ba.tz, tzf.tz, 'Asia/Bangkok'))) AS local_date
  FROM tx t
  LEFT JOIN `kz-kura.int_dw.brand_account` ba
    ON ba.account_id = t.accountId
  LEFT JOIN tz_fallback tzf
    ON tzf.country = UPPER(LEFT(t.reqCurrency, 2))
  WHERE @selected_country IS NULL
     OR COALESCE(UPPER(ba.country), UPPER(LEFT(t.reqCurrency, 2))) = @selected_country
),

cur AS (
  SELECT 'CUR' AS period, b.*
  FROM base b, bounds d
  WHERE b.local_date BETWEEN d.cur_start AND d.cur_end
),

prev AS (
  SELECT 'PREV' AS period, b.*
  FROM base b, bounds d
  WHERE b.local_date BETWEEN d.prev_start AND d.prev_end
),

all_tx AS (
  SELECT * FROM cur
  UNION ALL
  SELECT * FROM prev
)

SELECT
  period,
  CASE WHEN type = 'deposit'  THEN 'DEPOSIT'
       WHEN type = 'withdraw' THEN 'WITHDRAWAL'
  END AS tnx_type,
  providerKey,
  method,
  brand_name AS brand,
  status,
  country,
  AVG(TIMESTAMP_DIFF(completedAt, createdAt, SECOND))            AS avg_diff_seconds_transaction,
  COUNT(*)                                                       AS total_count,
  COUNTIF(TIMESTAMP_DIFF(completedAt, createdAt, SECOND) < 180)  AS transaction_within_180s,
  COUNTIF(TIMESTAMP_DIFF(completedAt, createdAt, SECOND) < 300)  AS transaction_within_300s,
  COUNTIF(TIMESTAMP_DIFF(completedAt, createdAt, SECOND) < 900)  AS transaction_within_900s
FROM all_tx
GROUP BY period, tnx_type, providerKey, method, brand, status, country
ORDER BY period, country, brand;
