-- Full completed deposits of LOCAL yesterday (per country), optionally filtered by PGW prefix.
-- Uses the same local-date basis as DPF:
-- realtime: insertedAt converted by country timezone
-- gold: timestamps are already in local time, so no conversion is applied.
--
-- Query parameters (bind at runtime):
--   @target_country : 2-letter country code (e.g. 'TH'), or NULL for all
--   @selected_pgw   : PGW name prefix (e.g. 'dpp'), or NULL for all

WITH country_clock AS (
  SELECT 'TH' AS country, '+07:00' AS tz_offset UNION ALL
  SELECT 'PH' AS country, '+08:00' AS tz_offset UNION ALL
  SELECT 'BD' AS country, '+06:00' AS tz_offset UNION ALL
  SELECT 'PK' AS country, '+05:00' AS tz_offset UNION ALL
  SELECT 'BR' AS country, '-03:00' AS tz_offset UNION ALL
  SELECT 'CO' AS country, '-05:00' AS tz_offset UNION ALL
  SELECT 'MX' AS country, '-06:00' AS tz_offset
),

-- 1. Determine Local "Today" for each country based on execution time
country_now AS (
  SELECT
    country,
    tz_offset,
    DATE(DATETIME(CURRENT_TIMESTAMP(), tz_offset)) AS today_date
  FROM country_clock
),

source_realtime AS (
  SELECT
    f.orderRef                       AS dedup_key,
    f.createdAt                    AS ts,
    UPPER(LEFT(f.reqCurrency, 2))    AS country,
    CAST(f.netAmount AS FLOAT64)     AS netAmount,
    CASE
      WHEN UPPER(f.method) LIKE '%DUMPLING%' THEN 'DPP'
      ELSE UPPER(f.method)
    END                              AS method,
    'realtime'                       AS src
  FROM `kz-dp-prod.kz_pg_to_bq_realtime.ext_funding_tx` AS f
  WHERE f.type = 'deposit'
    AND f.status = 'completed'
    AND f.netAmount IS NOT NULL
    AND (@target_country IS NULL OR UPPER(LEFT(f.reqCurrency, 2)) = @target_country)
    AND NOT (
      LOWER(COALESCE(@selected_pgw, '')) IN ('dpp', 'dumpling')
      AND UPPER(LEFT(f.reqCurrency, 2)) IN ('TH', 'PH')
    )
    AND (
      @selected_pgw IS NULL
      OR LOWER(COALESCE(f.method, '')) LIKE CONCAT(LOWER(@selected_pgw), '%')
      OR (LOWER(@selected_pgw) IN ('dpp', 'dumpling') AND (LOWER(COALESCE(f.method, '')) LIKE '%dumpling%' OR LOWER(COALESCE(f.method, '')) LIKE '%dpp%'))
    )
    -- Wide scan range to cover timezone boundaries safely
    AND f.insertedAt >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)), INTERVAL 8 HOUR)
    AND f.insertedAt <  TIMESTAMP_ADD(TIMESTAMP(CURRENT_DATE()), INTERVAL 6 HOUR)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY COALESCE(f.orderRef, CAST(f.id AS STRING))
    ORDER BY f.updatedAt DESC
  ) = 1
),

-- Distinct realtime order keys -> clean anti-join, no fan-out.
realtime_keys AS (
  SELECT DISTINCT dedup_key
  FROM source_realtime
  WHERE dedup_key IS NOT NULL
),

source_gold_backfill AS (
  SELECT
    d.order_id                                    AS dedup_key,
    SAFE_CAST(d.datetime_of_deposit AS TIMESTAMP) AS ts,
    UPPER(d.country)                              AS country,
    CAST(d.deposit_amount AS FLOAT64)             AS netAmount,
    CASE
      WHEN UPPER(d.payment_channel) LIKE '%DUMPLING%' THEN 'DPP'
      ELSE UPPER(d.payment_channel)
    END                                           AS method,
    'gold'                                        AS src
  FROM `kz-dp-prod.crm_gold_prod.deposit_transaction_consolidated` d
  WHERE NOT EXISTS (
      SELECT 1 FROM realtime_keys k WHERE k.dedup_key = d.order_id
    )
    AND (@target_country IS NULL OR UPPER(d.country) = @target_country)
    AND NOT (
      LOWER(COALESCE(@selected_pgw, '')) IN ('dpp', 'dumpling')
      AND UPPER(d.country) IN ('TH', 'PH')
    )
    AND (
      @selected_pgw IS NULL
      OR LOWER(COALESCE(d.payment_channel, '')) LIKE CONCAT(LOWER(@selected_pgw), '%')
      OR (LOWER(@selected_pgw) IN ('dpp', 'dumpling') AND (LOWER(COALESCE(d.payment_channel, '')) LIKE '%dumpling%' OR LOWER(COALESCE(d.payment_channel, '')) LIKE '%dpp%'))
    )
    -- Wide scan range matching realtime
    AND SAFE_CAST(d.datetime_of_deposit AS TIMESTAMP) >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)), INTERVAL 8 HOUR)
    AND SAFE_CAST(d.datetime_of_deposit AS TIMESTAMP) <  TIMESTAMP_ADD(TIMESTAMP(CURRENT_DATE()), INTERVAL 6 HOUR)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY d.order_id
    ORDER BY SAFE_CAST(d.datetime_of_deposit AS TIMESTAMP) DESC
  ) = 1
),

source_dpp_thph AS (
  SELECT
    CAST(NULL AS STRING)                          AS dedup_key,
    SAFE_CAST(d.completed_datetime AS TIMESTAMP)  AS ts,
    'TH'                                          AS country,
    CAST(d.dep_amount AS FLOAT64)                 AS netAmount,
    'DPP'                                         AS method,
    'dpp_gold'                                    AS src
  FROM `kz-dp-prod.dpp_gold_prod.th_dpp_deposit_gold` d
  WHERE d.status = 'success'
    AND LOWER(COALESCE(@selected_pgw, '')) IN ('dpp', 'dumpling')
    AND (@target_country IS NULL OR @target_country = 'TH')
    AND SAFE_CAST(d.completed_datetime AS TIMESTAMP) >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)), INTERVAL 8 HOUR)
    AND SAFE_CAST(d.completed_datetime AS TIMESTAMP) <  TIMESTAMP_ADD(TIMESTAMP(CURRENT_DATE()), INTERVAL 6 HOUR)

  UNION ALL

  SELECT
    CAST(NULL AS STRING)                          AS dedup_key,
    SAFE_CAST(d.completed_datetime AS TIMESTAMP)  AS ts,
    'PH'                                          AS country,
    CAST(d.dep_amount AS FLOAT64)                 AS netAmount,
    'DPP'                                         AS method,
    'dpp_gold'                                    AS src
  FROM `kz-dp-prod.dpp_gold_prod.ph_dpp_deposit_gold` d
  WHERE d.status = 'success'
    AND LOWER(COALESCE(@selected_pgw, '')) IN ('dpp', 'dumpling')
    AND (@target_country IS NULL OR @target_country = 'PH')
    AND SAFE_CAST(d.completed_datetime AS TIMESTAMP) >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)), INTERVAL 8 HOUR)
    AND SAFE_CAST(d.completed_datetime AS TIMESTAMP) <  TIMESTAMP_ADD(TIMESTAMP(CURRENT_DATE()), INTERVAL 6 HOUR)
),

source_dpp_thph_realtime_missing AS (
  SELECT
    COALESCE(f.orderRef, CAST(f.id AS STRING))   AS dedup_key,
    f.createdAt                                 AS ts,
    UPPER(LEFT(f.reqCurrency, 2))                 AS country,
    CAST(f.netAmount AS FLOAT64)                  AS netAmount,
    'DPP'                                         AS method,
    'realtime'                                    AS src
  FROM `kz-dp-prod.kz_pg_to_bq_realtime.ext_funding_tx` AS f
  WHERE f.type = 'deposit'
    AND f.status = 'completed'
    AND f.netAmount IS NOT NULL
    AND LOWER(COALESCE(@selected_pgw, '')) IN ('dpp', 'dumpling')
    AND UPPER(LEFT(f.reqCurrency, 2)) IN ('TH', 'PH')
    AND (@target_country IS NULL OR UPPER(LEFT(f.reqCurrency, 2)) = @target_country)
    AND (
      LOWER(COALESCE(f.method, '')) LIKE CONCAT(LOWER(@selected_pgw), '%')
      OR (LOWER(@selected_pgw) IN ('dpp', 'dumpling') AND (LOWER(COALESCE(f.method, '')) LIKE '%dumpling%' OR LOWER(COALESCE(f.method, '')) LIKE '%dpp%'))
    )
    AND (
      (UPPER(LEFT(f.reqCurrency, 2)) = 'TH' AND NOT EXISTS (
        SELECT 1
        FROM `kz-dp-prod.dpp_gold_prod.th_dpp_deposit_gold` d
        WHERE d.status = 'success'
          AND UPPER(CAST(d.order_id AS STRING)) = UPPER(COALESCE(f.orderRef, CAST(f.id AS STRING)))
          AND SAFE_CAST(d.completed_datetime AS TIMESTAMP) >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)), INTERVAL 8 HOUR)
          AND SAFE_CAST(d.completed_datetime AS TIMESTAMP) <  TIMESTAMP_ADD(TIMESTAMP(CURRENT_DATE()), INTERVAL 6 HOUR)
      ))
      OR
      (UPPER(LEFT(f.reqCurrency, 2)) = 'PH' AND NOT EXISTS (
        SELECT 1
        FROM `kz-dp-prod.dpp_gold_prod.ph_dpp_deposit_gold` d
        WHERE d.status = 'success'
          AND UPPER(CAST(d.order_id AS STRING)) = UPPER(COALESCE(f.orderRef, CAST(f.id AS STRING)))
          AND SAFE_CAST(d.completed_datetime AS TIMESTAMP) >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)), INTERVAL 8 HOUR)
          AND SAFE_CAST(d.completed_datetime AS TIMESTAMP) <  TIMESTAMP_ADD(TIMESTAMP(CURRENT_DATE()), INTERVAL 6 HOUR)
      ))
    )
    AND f.insertedAt >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)), INTERVAL 8 HOUR)
    AND f.insertedAt <  TIMESTAMP_ADD(TIMESTAMP(CURRENT_DATE()), INTERVAL 6 HOUR)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY COALESCE(f.orderRef, CAST(f.id AS STRING))
    ORDER BY f.updatedAt DESC
  ) = 1
),

combined AS (
  SELECT * FROM source_realtime
  UNION ALL
  SELECT * FROM source_gold_backfill
  UNION ALL
  SELECT * FROM source_dpp_thph
  UNION ALL
  SELECT * FROM source_dpp_thph_realtime_missing
),

-- 2. Apply Timezone logic per your strict requirements
base AS (
  SELECT
    cn.country,
    CASE 
      -- UTC to Local conversion using the specific country offset
      WHEN c.src = 'realtime' THEN DATE(DATETIME(c.ts, cn.tz_offset))
      
      -- Gold is already local. DATE() extracts the wall-clock day directly without timezone shifting.
      ELSE DATE(c.ts)
    END AS local_date,
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

-- 3. Filter exactly for local yesterday
SELECT
  n.country,
  DATE_SUB(n.today_date, INTERVAL 1 DAY) AS yesterday_date,
  COALESCE(a.full_yesterday_total, 0)    AS full_yesterday_total
FROM country_now n
LEFT JOIN agg a
  ON a.country    = n.country
 AND a.local_date = DATE_SUB(n.today_date, INTERVAL 1 DAY)
WHERE (@target_country IS NULL OR n.country = @target_country)
ORDER BY n.country;
