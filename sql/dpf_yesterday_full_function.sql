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
source_realtime AS (
  SELECT
    f.orderRef AS dedup_key,
    f.completedAt AS ts,
    UPPER(LEFT(f.reqCurrency, 2)) AS country,
    CAST(f.netAmount AS FLOAT64) AS netAmount,
    CASE
      WHEN UPPER(f.method) LIKE '%DUMPLING%' THEN 'DPP'
      ELSE UPPER(f.method)
    END AS method
  FROM `kz-dp-prod.kz_pg_to_bq_realtime.ext_funding_tx` AS f
  WHERE f.type = 'deposit'
    AND f.status = 'completed'
    AND f.netAmount IS NOT NULL
    AND (@target_country IS NULL OR UPPER(LEFT(f.reqCurrency, 2)) = @target_country)
    AND (
      @selected_pgw IS NULL
      OR LOWER(COALESCE(f.method, '')) LIKE CONCAT(LOWER(@selected_pgw), '%')
      OR (LOWER(@selected_pgw) IN ('dpp', 'dumpling') AND (LOWER(COALESCE(f.method, '')) LIKE '%dumpling%' OR LOWER(COALESCE(f.method, '')) LIKE '%dpp%'))
    )
    -- Scan range wide enough to safely cover yesterday across supported timezones (+8 to -6).
    AND f.insertedAt >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)), INTERVAL 8 HOUR)
    AND f.insertedAt <  TIMESTAMP_ADD(TIMESTAMP(CURRENT_DATE()), INTERVAL 6 HOUR)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY f.id ORDER BY f.updatedAt DESC) = 1
),
source_gold_backfill AS (
  SELECT
    d.order_id AS dedup_key,
    SAFE_CAST(d.datetime_of_deposit AS TIMESTAMP) AS ts,
    UPPER(d.country) AS country,
    CAST(d.deposit_amount AS FLOAT64) AS netAmount,
    CASE
      WHEN UPPER(d.payment_channel) LIKE '%DUMPLING%' THEN 'DPP'
      ELSE UPPER(d.payment_channel)
    END AS method
  FROM `kz-dp-prod.crm_gold_prod.deposit_transaction_consolidated` d
  LEFT JOIN source_realtime r
    ON r.dedup_key = d.order_id
  WHERE r.dedup_key IS NULL
    AND (@target_country IS NULL OR UPPER(d.country) = @target_country)
    AND (
      @selected_pgw IS NULL
      OR LOWER(COALESCE(d.payment_channel, '')) LIKE CONCAT(LOWER(@selected_pgw), '%')
      OR (LOWER(@selected_pgw) IN ('dpp', 'dumpling') AND (LOWER(COALESCE(d.payment_channel, '')) LIKE '%dumpling%' OR LOWER(COALESCE(d.payment_channel, '')) LIKE '%dpp%'))
    )
    AND SAFE_CAST(d.datetime_of_deposit AS TIMESTAMP) >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)), INTERVAL 8 HOUR)
    AND SAFE_CAST(d.datetime_of_deposit AS TIMESTAMP) <  TIMESTAMP_ADD(TIMESTAMP(CURRENT_DATE()), INTERVAL 6 HOUR)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY d.order_id
    ORDER BY SAFE_CAST(d.datetime_of_deposit AS TIMESTAMP) DESC
  ) = 1
),
combined AS (
  SELECT * FROM source_realtime
  UNION ALL
  SELECT * FROM source_gold_backfill
),
base AS (
  SELECT
    cn.country,
    DATE(DATETIME(c.ts, cn.tz_offset)) AS local_date,
    c.netAmount
  FROM combined c
  JOIN country_now cn
    ON cn.country = c.country
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
