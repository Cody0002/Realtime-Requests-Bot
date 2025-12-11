-- 3-day sliding window: today, -1d, -2d; each day's data capped at "now" in Asia/Bangkok
WITH params AS (
  SELECT
    'Asia/Bangkok' AS tz,
    CURRENT_DATE('Asia/Bangkok') AS today_date,
    CURRENT_TIME('Asia/Bangkok') AS now_time
),

-- Pull only deposits in the last 3 local days (UTC-pruned)
base AS (
  SELECT
    DATE(DATETIME(f.insertedAt, p.tz)) AS local_date,    -- e.g. 2025-08-29
    TIME(DATETIME(f.insertedAt, p.tz)) AS local_time,    -- time in Asia/Bangkok
    f.netAmount,
    f.netCurrency,
    f.reqCurrency,
    UPPER(a.name)  AS brand,       -- ✅ brand = account.name
    UPPER(a.`group`) AS `group`,     -- ✅ group = account.group
    LEFT(reqCurrency, 2) AS country
  FROM `kz-dp-prod.kz_pg_to_bq_realtime.ext_funding_tx` AS f
  LEFT JOIN `kz-dp-prod.kz_pg_to_bq_realtime.account` a
    ON f.accountId = a.id
  CROSS JOIN params p
  WHERE f.type   = 'deposit'
    AND f.status = 'completed'
    -- UTC bounds covering [today-2 00:00 .. today+1 00:00) local
    AND f.insertedAt >= TIMESTAMP(DATETIME(DATE_SUB(p.today_date, INTERVAL 2 DAY), TIME '00:00:00'), p.tz)
    AND f.insertedAt <  TIMESTAMP(DATETIME(DATE_ADD(p.today_date,  INTERVAL 1 DAY), TIME '00:00:00'), p.tz)
    ----------------------------------------------------------------------
    -- OPTIMIZATION: Filter by country at the earliest possible step.
    AND (@target_country IS NULL OR
          LEFT(reqCurrency, 2) = @target_country)
    ----------------------------------------------------------------------
  -- DEDUPLICATION: Keep only the latest record for each transaction ID.
  QUALIFY ROW_NUMBER() OVER (PARTITION BY f.id ORDER BY f.updatedAt DESC) = 1
    ----------------------------------------------------------------------
),

-- Cap every of the last 3 days at the SAME clock time (now_time in Bangkok)
capped AS (
  SELECT
    local_date AS date,
    country,
    brand,
    `group`,
    netAmount
  FROM base, params p
  WHERE local_date BETWEEN DATE_SUB(p.today_date, INTERVAL 2 DAY) AND p.today_date
    AND local_time < p.now_time
    AND netAmount IS NOT NULL
),

-- Aggregate per day, per group, per brand, per country
consolidated AS (
  SELECT
    date,
    country,
    `group`,
    brand,
    AVG(netAmount) AS AverageDeposit,
    SUM(netAmount) AS TotalDeposit
  FROM capped
  GROUP BY date, country, `group`, brand
),

today_total AS (
  SELECT country, `group`, brand, TotalDeposit AS TotalToday
  FROM consolidated, params p
  WHERE date = p.today_date
)

SELECT
  c.date,
  c.country,
  c.`group`,
  c.brand,
  c.AverageDeposit,
  ROUND(c.TotalDeposit, 0) AS TotalDeposit,
  ROUND(c.TotalDeposit / NULLIF(t.TotalToday, 0), 4) AS Weightage
FROM consolidated c
LEFT JOIN today_total t
  ON c.country = t.country
 AND c.`group` = t.`group`
 AND c.brand   = t.brand
--------------------------------------------------------------
-- This filter is no longer needed here.
-- WHERE @target_country IS NULL OR c.country = @target_country
--------------------------------------------------------------
ORDER BY c.date DESC, c.TotalDeposit DESC;