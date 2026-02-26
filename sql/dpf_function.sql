-- 3-day sliding window: today, -1d, -2d; each day capped at each country's local "now"
WITH country_clock AS (
  SELECT 'TH' AS country, '+07:00' AS tz_offset UNION ALL
  SELECT 'PH' AS country, '+08:00' AS tz_offset UNION ALL
  SELECT 'BD' AS country, '+06:00' AS tz_offset UNION ALL
  SELECT 'PK' AS country, '+05:00' AS tz_offset UNION ALL
  SELECT 'BR' AS country, '-03:00' AS tz_offset UNION ALL
  SELECT 'CO' AS country, '-05:00' AS tz_offset UNION ALL
  SELECT 'MX' AS country, '-06:00' AS tz_offset
),
country_now AS (
  SELECT
    country,
    tz_offset,
    DATE(DATETIME(CURRENT_TIMESTAMP(), tz_offset)) AS today_date,
    TIME(DATETIME(CURRENT_TIMESTAMP(), tz_offset)) AS now_time
  FROM country_clock
),
base AS (
  SELECT
    DATE(DATETIME(f.insertedAt, cn.tz_offset)) AS local_date,
    TIME(DATETIME(f.insertedAt, cn.tz_offset)) AS local_time,
    cn.today_date,
    cn.now_time,
    f.netAmount,
    f.netCurrency,
    f.reqCurrency,
    UPPER(a.name) AS brand,
    UPPER(a.`group`) AS `group`,
    cn.country
  FROM `kz-dp-prod.kz_pg_to_bq_realtime.ext_funding_tx` AS f
  LEFT JOIN `kz-dp-prod.kz_pg_to_bq_realtime.account` a
    ON f.accountId = a.id
  JOIN country_now cn
    ON cn.country = LEFT(f.reqCurrency, 2)
  WHERE f.type = 'deposit'
    AND f.status = 'completed'
    -- Range wide enough to cover supported local timezones (+8 to -6).
    AND f.insertedAt >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY)), INTERVAL 8 HOUR)
    AND f.insertedAt <  TIMESTAMP_ADD(TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)), INTERVAL 6 HOUR)
    AND (@target_country IS NULL OR cn.country = @target_country)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY f.id ORDER BY f.updatedAt DESC) = 1
),
capped AS (
  SELECT
    local_date AS date,
    country,
    brand,
    `group`,
    netAmount
  FROM base
  WHERE local_date BETWEEN DATE_SUB(today_date, INTERVAL 3 DAY) AND today_date
    AND local_time < now_time
    AND netAmount IS NOT NULL
),
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
  SELECT
    c.country,
    c.`group`,
    c.brand,
    c.TotalDeposit AS TotalToday
  FROM consolidated c
  JOIN country_now n
    ON n.country = c.country
  WHERE c.date = n.today_date
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
ORDER BY c.date DESC, c.TotalDeposit DESC;
