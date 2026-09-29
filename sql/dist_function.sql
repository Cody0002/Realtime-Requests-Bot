-- =====================================================================
-- DIST (Deposit Channel Distribution) — KURA VERSION
-- =====================================================================
-- Drop-in replacement for the kz-dp-prod dist_function.sql. Same params,
-- same output columns.
--
--   Data project : kz-kura           (location US)
--   Job project  : kz-dp-ops
--   Realtime     : kz-kura.prod_dw.fundingTx
--   Brand dim    : kz-kura.int_dw.brand_account
--
-- Params:
--   @target_date      DATE    exact LOCAL date to report
--   @selected_country STRING  2-letter country code, or NULL for all
--   @selected_pgw     STRING  PGW prefix (e.g. 'DPP'), or NULL for all methods
--
-- ⚠ RETENTION: prod_dw.fundingTx keeps roughly the last 50 days (partitioned on
-- insertedAt). That covers the 7–14 day window /dist is actually used for, but a
-- date older than that returns NO ROWS rather than an error. If deep history is
-- ever needed, kz-kura.int_dw.fundingTx holds it (partitioned on requestedAt) at
-- the cost of an hourly refresh lag.
--
-- What changed vs the kz-dp-prod version:
--   * source       : ext_funding_tx -> prod_dw.fundingTx (same columns)
--   * country / tz : int_dw.brand_account instead of the hardcoded currency ->
--                    offset CASE (kept only as a fallback)
--   * date filter  : the old query matched the local date on insertedAt. In Kura
--                    insertedAt is the landing/update time — rows can re-land long
--                    after they are created — so only createdAt (the business time)
--                    is matched now; insertedAt is used purely as a lower partition
--                    bound (a row can never land before it is created).
--   * PGW filter   : 'dpp' / 'dumpling' match on method OR providerKey, like DPF
--   * soft deletes : deletedAt IS NULL
--   * dedup        : QUALIFY on id, newest updatedAt wins
--
-- method values are IDENTICAL to the old source — same "<provider>/<paymentMethod>"
-- format (e.g. 'dpp-ph/gcash-qr', 's11slippay/bank-transfer-native').
--
-- Output: country, method, currency, deposit_tnx_count,
--         total_deposit_amount_native, average_deposit_amount_native,
--         pct_of_country_total_native  (unchanged)
-- =====================================================================

WITH
-- Wide enough to cover the target local day in any timezone from UTC-6 to UTC+8.
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

deposits AS (
  SELECT
    f.accountId,
    f.createdAt,
    f.netAmount,
    f.reqCurrency,
    f.method
  FROM `kz-kura.prod_dw.fundingTx` f
  CROSS JOIN bounds b
  WHERE f.insertedAt >= b.lo                        -- partition lower bound only
    AND f.createdAt  >= b.lo AND f.createdAt < b.hi -- business time
    AND f.type      = 'deposit'
    AND f.status    = 'completed'
    AND f.deletedAt IS NULL
    AND f.reqCurrency IS NOT NULL
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

labelled AS (
  SELECT
    COALESCE(UPPER(ba.country), UPPER(LEFT(d.reqCurrency, 2))) AS country,
    COALESCE(ba.tz, tzf.tz, 'Asia/Bangkok')                     AS tz,
    COALESCE(d.method, 'UNKNOWN')                               AS method,
    d.reqCurrency                                               AS currency,
    CAST(d.netAmount AS FLOAT64)                                AS net_amount,
    d.createdAt
  FROM deposits d
  LEFT JOIN `kz-kura.int_dw.brand_account` ba
    ON ba.account_id = d.accountId
  LEFT JOIN tz_fallback tzf
    ON tzf.country = UPPER(LEFT(d.reqCurrency, 2))
),

on_date AS (
  SELECT country, method, currency, net_amount
  FROM labelled
  WHERE DATE(DATETIME(createdAt, tz)) = @target_date
    AND (@selected_country IS NULL OR country = @selected_country)
),

grouped AS (
  SELECT
    country,
    method,
    currency,
    COUNT(*)        AS deposit_tnx_count,
    SUM(net_amount) AS total_native,
    AVG(net_amount) AS avg_native
  FROM on_date
  GROUP BY country, method, currency
)

SELECT
  country,
  method,
  currency,
  deposit_tnx_count,
  ROUND(total_native, 0) AS total_deposit_amount_native,
  ROUND(avg_native, 0)   AS average_deposit_amount_native,
  CONCAT(
    ROUND(
      SAFE_DIVIDE(total_native * 100.0, SUM(total_native) OVER (PARTITION BY country)),
      2
    ),
    '%'
  ) AS pct_of_country_total_native
FROM grouped
ORDER BY country, total_deposit_amount_native DESC, method;
