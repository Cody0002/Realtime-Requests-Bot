-- =====================================================================
-- DPF full local-yesterday totals — KURA VERSION
-- =====================================================================
-- Full completed deposits of LOCAL yesterday (per country), optionally filtered
-- by PGW prefix. Used as the baseline for the DPP estimation sentence in /dpf
-- and /dist. Same parameters and output columns as the kz-dp-prod version.
--
--   Data project : kz-kura           (location US)
--   Job project  : kz-dp-ops
--   Realtime     : kz-kura.prod_dw.fundingTx
--   Brand dim    : kz-kura.int_dw.brand_account
--
-- Params:
--   @target_country : 2-letter country code (e.g. 'TH'), or NULL for all
--   @selected_pgw   : PGW name prefix (e.g. 'dpp'), or NULL for all
--
-- What changed vs the kz-dp-prod version:
--   * SOURCE 1 realtime : ext_funding_tx -> prod_dw.fundingTx
--   * SOURCE 2 crm_gold : REMOVED (Kura reads the production DB directly)
--   * SOURCE 3/4 DPP    : REMOVED. No dpp_gold in Kura — DPP comes from
--                         method / providerKey on fundingTx for every country.
--   * timezone          : per-brand brand_account.tz (fallback: IANA list below)
--   * dedup             : QUALIFY on id, newest updatedAt wins
--   * soft deletes      : deletedAt IS NULL
--
-- Output: country, yesterday_date, full_yesterday_total  (unchanged)
--         One row per supported country; 0 when there were no deposits.
-- =====================================================================

WITH
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

-- Country spine so every requested country returns a row (0 when empty),
-- matching the old LEFT JOIN from country_now.
country_spine AS (
  SELECT
    country,
    DATE(DATETIME(CURRENT_TIMESTAMP(), tz)) AS today_date
  FROM tz_fallback
  WHERE @target_country IS NULL OR country = @target_country
),

-- ============================================================
-- Completed deposits, deduplicated, optional PGW filter
-- ============================================================
funding AS (
  SELECT
    f.id,
    f.accountId,
    f.createdAt,
    f.netAmount,
    f.reqCurrency
  FROM `kz-kura.prod_dw.fundingTx` AS f
  WHERE f.type      = 'deposit'
    AND f.status    = 'completed'
    AND f.deletedAt IS NULL
    AND f.netAmount IS NOT NULL
    -- Partition filter (BigQuery requires one on insertedAt). It must be a constant
    -- expression written inline here: a bound taken from a CTE is not used for
    -- partition elimination and the query is rejected.
    -- Local "yesterday" across UTC-6..UTC+8 never reaches back more than ~48h.
    AND f.insertedAt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 4 DAY)
    AND f.createdAt  >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 4 DAY)
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
    COALESCE(UPPER(ba.country), UPPER(LEFT(f.reqCurrency, 2))) AS country,
    COALESCE(ba.tz, tzf.tz, 'Asia/Bangkok')                     AS tz,
    CAST(f.netAmount AS FLOAT64)                                AS netAmount,
    f.createdAt
  FROM funding f
  LEFT JOIN `kz-kura.int_dw.brand_account` ba
    ON ba.account_id = f.accountId
  LEFT JOIN tz_fallback tzf
    ON tzf.country = UPPER(LEFT(f.reqCurrency, 2))
  WHERE @target_country IS NULL
     OR COALESCE(UPPER(ba.country), UPPER(LEFT(f.reqCurrency, 2))) = @target_country
),

localised AS (
  SELECT
    country,
    netAmount,
    DATE(DATETIME(createdAt, tz))           AS local_date,
    DATE(DATETIME(CURRENT_TIMESTAMP(), tz)) AS today_date
  FROM labelled
),

agg AS (
  SELECT
    country,
    local_date,
    ROUND(SUM(netAmount), 0) AS full_yesterday_total
  FROM localised
  WHERE local_date = DATE_SUB(today_date, INTERVAL 1 DAY)
  GROUP BY country, local_date
)

SELECT
  COALESCE(s.country, a.country)                                   AS country,
  COALESCE(DATE_SUB(s.today_date, INTERVAL 1 DAY), a.local_date)   AS yesterday_date,
  COALESCE(a.full_yesterday_total, 0)                              AS full_yesterday_total
FROM country_spine s
FULL OUTER JOIN agg a
  ON a.country = s.country
ORDER BY 1;
