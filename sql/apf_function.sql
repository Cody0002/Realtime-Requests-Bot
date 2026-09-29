-- =====================================================================
-- APF (Acquisition Performance) — KURA VERSION
-- =====================================================================
-- Drop-in replacement for the kz-dp-prod apf_function.sql. Same parameter,
-- same output columns.
--
--   Data project : kz-kura           (location US)
--   Job project  : kz-dp-ops
--   Registrations: kz-kura.prod_dw.member         (raw landing of the app member table;
--                                                  was kz_pg_to_bq_realtime.ext_member)
--   Deposits     : kz-kura.prod_dw.fundingTx      (was kz_pg_to_bq_realtime.ext_funding_tx)
--   Brand dim    : kz-kura.int_dw.brand_account   (account_id -> brand/groupName/country/tz;
--                                                  replaces the account table + funding-derived
--                                                  brand->country map)
--
-- Param: @target_country STRING (NULL = all)
--
-- FIRST DEPLOY CHECK: prod_dw.member is the only Kura object not already
-- exercised by the reference (Lark) bot. Verify it on the server before restarting:
--   bq --project_id=kz-dp-ops --location=US show --schema kz-kura:prod_dw.member
-- Columns used: id, accountId, registerAt, insertedAt.
--
-- What changed vs the kz-dp-prod version:
--   * 3-day sliding window (today, -1d, -2d), each day capped at the brand's
--     local "now" — same semantics, but the clock comes from brand_account.tz
--     instead of a hardcoded offset list.
--   * NAR counts DISTINCT member ids (was CONCAT(gamePrefix, apiIdentifier),
--     which is unique per member anyway) so the account table is not needed.
--   * FTD/STD/TTD rank each member's completed deposits by createdAt within the
--     same scan window (unchanged from the current kz-dp-prod query) and keep the
--     ones whose completedAt falls inside each day's partial window.
--   * dedup: QUALIFY on id (raw landing can repeat a row); soft deletes excluded
--     on fundingTx (deletedAt IS NULL).
--   * Registrations whose account is missing from brand_account are dropped
--     (no country/timezone to place them in). Deposits fall back to
--     LEFT(reqCurrency, 2) for country and to the IANA list below for timezone.
--
-- Output: date, group, brand, country, NAR, FTD, STD, TTD  (unchanged)
-- =====================================================================

WITH
-- Same UTC scan window the kz-dp-prod query used: from 3 UTC days ago minus 8h
-- (start of local today-2 in UTC+8) up to now. Also the base for deposit ranking.
global_window AS (
  SELECT
    TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)), INTERVAL 8 HOUR) AS lo,
    CURRENT_TIMESTAMP()                                                                 AS hi
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

-- One row per account: brand, group, country and local timezone.
accounts AS (
  SELECT
    ba.account_id,
    UPPER(ba.brand)                            AS brand,
    UPPER(ba.groupName)                        AS `group`,
    UPPER(ba.country)                          AS country,
    COALESCE(ba.tz, tzf.tz, 'Asia/Bangkok')    AS tz
  FROM `kz-kura.int_dw.brand_account` ba
  LEFT JOIN tz_fallback tzf
    ON tzf.country = UPPER(ba.country)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ba.account_id ORDER BY ba.brand) = 1
),

-- ============================================================
-- Registrations (NAR)
-- ============================================================
members AS (
  SELECT
    m.id        AS member_id,
    m.accountId,
    m.registerAt
  FROM `kz-kura.prod_dw.member` AS m
  CROSS JOIN global_window gw
  WHERE m.insertedAt >= gw.lo               -- landing time: never before registerAt
    AND m.registerAt >= gw.lo
    AND m.registerAt <  gw.hi
  QUALIFY ROW_NUMBER() OVER (PARTITION BY m.id ORDER BY m.registerAt DESC) = 1
),

registrations AS (
  SELECT
    DATE(DATETIME(m.registerAt, a.tz))           AS local_date,
    TIME(DATETIME(m.registerAt, a.tz))           AS local_time,
    DATE(DATETIME(CURRENT_TIMESTAMP(), a.tz))    AS today_date,
    TIME(DATETIME(CURRENT_TIMESTAMP(), a.tz))    AS now_time,
    a.country,
    a.`group`,
    a.brand,
    m.member_id
  FROM members m
  JOIN accounts a
    ON a.account_id = m.accountId
  WHERE @target_country IS NULL OR a.country = @target_country
),

consolidated_nar AS (
  SELECT
    local_date                 AS date,
    `group`,
    brand,
    country,
    COUNT(DISTINCT member_id)  AS NAR
  FROM registrations
  WHERE local_date BETWEEN DATE_SUB(today_date, INTERVAL 2 DAY) AND today_date
    AND local_time < now_time
  GROUP BY date, `group`, brand, country
),

-- ============================================================
-- Deposits (FTD / STD / TTD)
-- ============================================================
funding AS (
  SELECT
    f.id,
    f.memberId,
    f.accountId,
    f.createdAt,
    f.completedAt,
    f.reqCurrency
  FROM `kz-kura.prod_dw.fundingTx` AS f
  CROSS JOIN global_window gw
  WHERE f.type      = 'deposit'
    AND f.status    = 'completed'
    AND f.deletedAt IS NULL
    AND f.insertedAt >= gw.lo
    AND f.insertedAt <  gw.hi
    AND f.createdAt  >= gw.lo
    AND f.createdAt  <  gw.hi
  QUALIFY ROW_NUMBER() OVER (PARTITION BY f.id ORDER BY f.updatedAt DESC) = 1
),

labelled_deposit AS (
  SELECT
    f.id,
    f.memberId,
    f.createdAt,
    f.completedAt,
    COALESCE(a.country, UPPER(LEFT(f.reqCurrency, 2)))  AS country,
    COALESCE(a.`group`, 'UNKNOWN')                      AS `group`,
    COALESCE(a.brand, 'UNKNOWN')                        AS brand,
    COALESCE(a.tz, tzf.tz, 'Asia/Bangkok')              AS tz
  FROM funding f
  LEFT JOIN accounts a
    ON a.account_id = f.accountId
  LEFT JOIN tz_fallback tzf
    ON tzf.country = UPPER(LEFT(f.reqCurrency, 2))
  WHERE @target_country IS NULL
     OR COALESCE(a.country, UPPER(LEFT(f.reqCurrency, 2))) = @target_country
),

-- Rank each member's deposits inside the scan window (1st / 2nd / 3rd)
ranked_deposit AS (
  SELECT
    ld.*,
    RANK() OVER (PARTITION BY ld.memberId ORDER BY ld.createdAt ASC) AS rank_deposit
  FROM labelled_deposit ld
),

-- Keep deposits whose completedAt falls in each day's partial window (local clock)
windowed_deposit AS (
  SELECT
    DATE(DATETIME(completedAt, tz)) AS date,
    brand,
    `group`,
    country,
    rank_deposit
  FROM ranked_deposit
  WHERE completedAt IS NOT NULL
    AND DATE(DATETIME(completedAt, tz))
          BETWEEN DATE_SUB(DATE(DATETIME(CURRENT_TIMESTAMP(), tz)), INTERVAL 2 DAY)
              AND DATE(DATETIME(CURRENT_TIMESTAMP(), tz))
    AND TIME(DATETIME(completedAt, tz)) < TIME(DATETIME(CURRENT_TIMESTAMP(), tz))
),

consolidated_deposit AS (
  SELECT
    date,
    brand,
    `group`,
    country,
    COUNTIF(rank_deposit = 1) AS FTD,
    COUNTIF(rank_deposit = 2) AS STD,
    COUNTIF(rank_deposit = 3) AS TTD
  FROM windowed_deposit
  GROUP BY date, brand, `group`, country
),

brand_total AS (
  SELECT brand, SUM(NAR) AS total_nar
  FROM consolidated_nar
  GROUP BY brand
)

SELECT
  cn.date,
  cn.`group`,
  cn.brand,
  cn.country,
  cn.NAR,
  COALESCE(cd.FTD, 0) AS FTD,
  COALESCE(cd.STD, 0) AS STD,
  COALESCE(cd.TTD, 0) AS TTD
FROM consolidated_nar cn
LEFT JOIN consolidated_deposit cd
  ON  cn.date    = cd.date
 AND  cn.brand   = cd.brand
 AND  cn.`group` = cd.`group`
 AND  cn.country = cd.country
JOIN brand_total bt
  ON cn.brand = bt.brand
ORDER BY bt.total_nar DESC, cn.date DESC;
