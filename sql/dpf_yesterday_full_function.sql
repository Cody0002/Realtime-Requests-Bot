-- Full completed deposits of LOCAL yesterday (per country), optionally filtered by PGW prefix.
-- Uses the same local-date basis as DPF (insertedAt converted by country timezone).
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
    DATE(DATETIME(CURRENT_TIMESTAMP(), tz_offset)) AS today_date
  FROM country_clock
),
base AS (
  SELECT
    cn.country,
    DATE(DATETIME(f.insertedAt, cn.tz_offset)) AS local_date,
    CAST(f.netAmount AS FLOAT64) AS netAmount
  FROM `kz-dp-prod.kz_pg_to_bq_realtime.ext_funding_tx` AS f
  JOIN country_now cn
    ON cn.country = LEFT(f.reqCurrency, 2)
  WHERE f.type = 'deposit'
    AND f.status = 'completed'
    AND f.netAmount IS NOT NULL
    AND (@target_country IS NULL OR cn.country = @target_country)
    AND (@selected_pgw IS NULL OR LOWER(COALESCE(f.method, '')) LIKE CONCAT(LOWER(@selected_pgw), '%'))
    -- Scan range wide enough to safely cover yesterday across supported timezones (+8 to -6).
    AND f.insertedAt >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)), INTERVAL 8 HOUR)
    AND f.insertedAt <  TIMESTAMP_ADD(TIMESTAMP(CURRENT_DATE()), INTERVAL 6 HOUR)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY f.id ORDER BY f.updatedAt DESC) = 1
),
agg AS (
  SELECT
    country,
    local_date,
    ROUND(SUM(netAmount), 0) AS full_yesterday_total
  FROM base
  GROUP BY country, local_date
)
SELECT
  n.country,
  DATE_SUB(n.today_date, INTERVAL 1 DAY) AS yesterday_date,
  COALESCE(a.full_yesterday_total, 0) AS full_yesterday_total
FROM country_now n
LEFT JOIN agg a
  ON a.country = n.country
 AND a.local_date = DATE_SUB(n.today_date, INTERVAL 1 DAY)
WHERE (@target_country IS NULL OR n.country = @target_country)
ORDER BY n.country;
