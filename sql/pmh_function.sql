-- =====================================================================
-- PMH (Payment Health, single day) — KURA VERSION
-- =====================================================================
-- Drop-in replacement for the kz-dp-prod pmh_function.sql. Same params,
-- same output columns (brand mapping to group_name is still done in Python
-- from sql/brand_mapping.csv).
--
--   Data project : kz-kura           (location US)
--   Job project  : kz-dp-ops
--   Realtime     : kz-kura.prod_dw.fundingTx
--   Brand dim    : kz-kura.int_dw.brand_account
--
-- Params:
--   @target_date      DATE    exact LOCAL date to report
--   @selected_country STRING  2-letter country code, or NULL for all
--
-- ⚠ RETENTION: prod_dw.fundingTx keeps roughly the last 50 days. Older dates
-- return no rows.
--
-- What changed vs the kz-dp-prod version:
--   * source       : ext_funding_tx + account -> prod_dw.fundingTx + int_dw.brand_account
--   * brand        : brand_account.brand (UPPER) instead of account.name
--   * country / tz : brand_account (fallback: LEFT(reqCurrency,2) + IANA list)
--   * date filter  : local date of createdAt (insertedAt in Kura is landing time)
--   * soft deletes : deletedAt IS NULL
--   * dedup        : QUALIFY on id, newest updatedAt wins
--
-- Output: tnx_type, providerKey, method, brand, status, country,
--         avg_diff_seconds_transaction, total_count,
--         transaction_within_180s, transaction_within_300s, transaction_within_900s
-- =====================================================================

WITH
bounds AS (
  SELECT
    TIMESTAMP_SUB(TIMESTAMP(@target_date), INTERVAL 1 DAY) AS lo,
    TIMESTAMP_ADD(TIMESTAMP(@target_date), INTERVAL 2 DAY) AS hi
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

-- STEP 1: scan the base table (partition-pruned, deduplicated)
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
  CROSS JOIN bounds b
  WHERE f.type   IN ('deposit', 'withdraw')
    AND f.status IN ('completed', 'error', 'timeout', 'errors')
    AND f.deletedAt IS NULL
    AND f.insertedAt >= b.lo                        -- partition lower bound only
    AND f.createdAt  >= b.lo AND f.createdAt < b.hi -- business time
  QUALIFY ROW_NUMBER() OVER (PARTITION BY f.id ORDER BY f.updatedAt DESC) = 1
),

all_transactions AS (
  SELECT
    t.type,
    COALESCE(UPPER(ba.country), UPPER(LEFT(t.reqCurrency, 2))) AS country,
    t.createdAt,
    t.completedAt,
    t.providerKey,
    t.method,
    UPPER(ba.brand)                                             AS brand_name,
    CASE WHEN t.status = 'errors' THEN 'error' ELSE t.status END AS status,
    t.netAmount,
    COALESCE(ba.tz, tzf.tz, 'Asia/Bangkok')                     AS tz
  FROM tx t
  LEFT JOIN `kz-kura.int_dw.brand_account` ba
    ON ba.account_id = t.accountId
  LEFT JOIN tz_fallback tzf
    ON tzf.country = UPPER(LEFT(t.reqCurrency, 2))
),

on_date AS (
  SELECT *
  FROM all_transactions
  WHERE DATE(DATETIME(createdAt, tz)) = @target_date
    AND (@selected_country IS NULL OR country = @selected_country)
)

-- STEP 2: aggregate
SELECT
  CASE
    WHEN t.type = 'deposit'  THEN 'DEPOSIT'
    WHEN t.type = 'withdraw' THEN 'WITHDRAWAL'
  END AS tnx_type,
  t.providerKey,
  t.method,
  t.brand_name AS brand,
  t.status,
  t.country,
  AVG(TIMESTAMP_DIFF(t.completedAt, t.createdAt, SECOND))              AS avg_diff_seconds_transaction,
  COUNT(*)                                                             AS total_count,
  COUNTIF(TIMESTAMP_DIFF(t.completedAt, t.createdAt, SECOND) < 180)    AS transaction_within_180s,
  COUNTIF(TIMESTAMP_DIFF(t.completedAt, t.createdAt, SECOND) < 300)    AS transaction_within_300s,
  COUNTIF(TIMESTAMP_DIFF(t.completedAt, t.createdAt, SECOND) < 900)    AS transaction_within_900s
FROM on_date AS t
GROUP BY tnx_type, providerKey, method, brand, status, country;
