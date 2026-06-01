-- Query parameters (bind these at runtime via the API / BigQuery console):
--   @target_country : 2-letter country code (e.g. 'TH'), or NULL for all countries
--   @selected_pgw   : PGW name prefix (e.g. 'dpp'), or NULL for all
--                     'dpp' / 'dumpling' both match Dumpling-style methods

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

-- 1) Realtime source (primary).
--    Dedup on the ORDER key (orderRef), not f.id (f.id is unique -> no-op).
source_realtime AS (
  SELECT
    f.orderRef                       AS dedup_key,
    f.completedAt                    AS ts,
    f.netAmount,
    UPPER(a.name)                    AS brand,
    UPPER(a.`group`)                 AS `group`,
    UPPER(LEFT(f.reqCurrency, 2))    AS country,
    CASE
      WHEN UPPER(f.method) LIKE '%DUMPLING%' OR UPPER(f.method) LIKE '%DPP%' THEN 'DPP'
      ELSE UPPER(f.method)
    END                              AS method,
    'realtime'                       AS src
  FROM `kz-dp-prod.kz_pg_to_bq_realtime.ext_funding_tx` AS f
  LEFT JOIN `kz-dp-prod.kz_pg_to_bq_realtime.account` a
    ON f.accountId = a.id
  WHERE f.type = 'deposit'
    AND f.status = 'completed'
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
    AND f.insertedAt >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY)), INTERVAL 8 HOUR)
    AND f.insertedAt <  TIMESTAMP_ADD(TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)), INTERVAL 6 HOUR)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY COALESCE(f.orderRef, CAST(f.id AS STRING))
    ORDER BY f.updatedAt DESC
  ) = 1
),

-- Distinct realtime order keys for a clean anti-join (no fan-out).
realtime_keys AS (
  SELECT DISTINCT dedup_key
  FROM source_realtime
  WHERE dedup_key IS NOT NULL
),

-- One row per brand for the brand -> group lookup
-- (account.name is not unique, so collapse first to avoid fan-out).
account_group AS (
  SELECT
    UPPER(name)     AS brand,
    UPPER(`group`)  AS `group`
  FROM `kz-dp-prod.kz_pg_to_bq_realtime.account`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(name) ORDER BY id) = 1
),

-- 2) Gold backfill: only orders not present in realtime.
source_gold_backfill AS (
  SELECT
    d.order_id                                    AS dedup_key,
    SAFE_CAST(d.datetime_of_deposit AS TIMESTAMP) AS ts,
    CAST(d.deposit_amount AS FLOAT64)             AS netAmount,
    UPPER(d.brand)                                AS brand,
    ag.`group`                                    AS `group`,
    UPPER(d.country)                              AS country,
    CASE
      WHEN UPPER(d.payment_channel) LIKE '%DUMPLING%' OR UPPER(d.payment_channel) LIKE '%DPP%' THEN 'DPP'
      ELSE UPPER(d.payment_channel)
    END                                           AS method,
    'gold'                                        AS src
  FROM `kz-dp-prod.crm_gold_prod.deposit_transaction_consolidated` d
  LEFT JOIN account_group ag
    ON ag.brand = UPPER(d.brand)
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
    AND SAFE_CAST(d.datetime_of_deposit AS TIMESTAMP) >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY)), INTERVAL 8 HOUR)
    AND SAFE_CAST(d.datetime_of_deposit AS TIMESTAMP) <  TIMESTAMP_ADD(TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)), INTERVAL 6 HOUR)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY d.order_id
    ORDER BY SAFE_CAST(d.datetime_of_deposit AS TIMESTAMP) DESC
  ) = 1
),

source_dpp_thph AS (
  SELECT
    CAST(NULL AS STRING)                          AS dedup_key,
    SAFE_CAST(d.completed_datetime AS TIMESTAMP)  AS ts,
    CAST(d.dep_amount AS FLOAT64)                 AS netAmount,
    'DPP'                                         AS brand,
    'DPP'                                         AS `group`,
    'TH'                                          AS country,
    'DPP'                                         AS method,
    'dpp_gold'                                    AS src
  FROM `kz-dp-prod.dpp_gold_prod.th_dpp_deposit_gold` d
  WHERE d.status = 'success'
    AND LOWER(COALESCE(@selected_pgw, '')) IN ('dpp', 'dumpling')
    AND (@target_country IS NULL OR @target_country = 'TH')
    AND SAFE_CAST(d.completed_datetime AS TIMESTAMP) >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY)), INTERVAL 8 HOUR)
    AND SAFE_CAST(d.completed_datetime AS TIMESTAMP) <  TIMESTAMP_ADD(TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)), INTERVAL 6 HOUR)

  UNION ALL

  SELECT
    CAST(NULL AS STRING)                          AS dedup_key,
    SAFE_CAST(d.completed_datetime AS TIMESTAMP)  AS ts,
    CAST(d.dep_amount AS FLOAT64)                 AS netAmount,
    'DPP'                                         AS brand,
    'DPP'                                         AS `group`,
    'PH'                                          AS country,
    'DPP'                                         AS method,
    'dpp_gold'                                    AS src
  FROM `kz-dp-prod.dpp_gold_prod.ph_dpp_deposit_gold` d
  WHERE d.status = 'success'
    AND LOWER(COALESCE(@selected_pgw, '')) IN ('dpp', 'dumpling')
    AND (@target_country IS NULL OR @target_country = 'PH')
    AND SAFE_CAST(d.completed_datetime AS TIMESTAMP) >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY)), INTERVAL 8 HOUR)
    AND SAFE_CAST(d.completed_datetime AS TIMESTAMP) <  TIMESTAMP_ADD(TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)), INTERVAL 6 HOUR)
),

source_dpp_thph_realtime_missing AS (
  SELECT
    COALESCE(f.orderRef, CAST(f.id AS STRING))   AS dedup_key,
    f.completedAt                                 AS ts,
    CAST(f.netAmount AS FLOAT64)                  AS netAmount,
    'DPP'                                         AS brand,
    'DPP'                                         AS `group`,
    UPPER(LEFT(f.reqCurrency, 2))                 AS country,
    'DPP'                                         AS method,
    'realtime'                                    AS src
  FROM `kz-dp-prod.kz_pg_to_bq_realtime.ext_funding_tx` AS f
  WHERE f.type = 'deposit'
    AND f.status = 'completed'
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
          AND SAFE_CAST(d.completed_datetime AS TIMESTAMP) >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY)), INTERVAL 8 HOUR)
          AND SAFE_CAST(d.completed_datetime AS TIMESTAMP) <  TIMESTAMP_ADD(TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)), INTERVAL 6 HOUR)
      ))
      OR
      (UPPER(LEFT(f.reqCurrency, 2)) = 'PH' AND NOT EXISTS (
        SELECT 1
        FROM `kz-dp-prod.dpp_gold_prod.ph_dpp_deposit_gold` d
        WHERE d.status = 'success'
          AND UPPER(CAST(d.order_id AS STRING)) = UPPER(COALESCE(f.orderRef, CAST(f.id AS STRING)))
          AND SAFE_CAST(d.completed_datetime AS TIMESTAMP) >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY)), INTERVAL 8 HOUR)
          AND SAFE_CAST(d.completed_datetime AS TIMESTAMP) <  TIMESTAMP_ADD(TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)), INTERVAL 6 HOUR)
      ))
    )
    AND f.insertedAt >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY)), INTERVAL 8 HOUR)
    AND f.insertedAt <  TIMESTAMP_ADD(TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)), INTERVAL 6 HOUR)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY COALESCE(f.orderRef, CAST(f.id AS STRING))
    ORDER BY f.updatedAt DESC
  ) = 1
),

-- 3) Combine: realtime (1 row/order) + only the orders missing from realtime.
combined AS (
  SELECT * FROM source_realtime
  UNION ALL
  SELECT * FROM source_gold_backfill
  UNION ALL
  SELECT * FROM source_dpp_thph
  UNION ALL
  SELECT * FROM source_dpp_thph_realtime_missing
),

-- Realtime ts is UTC -> convert to local; gold ts is already local -> use as-is.
base AS (
  SELECT
    CASE WHEN c.src = 'realtime'
         THEN DATE(DATETIME(c.ts, cn.tz_offset))
         ELSE DATE(c.ts)
    END AS local_date,
    CASE WHEN c.src = 'realtime'
         THEN TIME(DATETIME(c.ts, cn.tz_offset))
         ELSE TIME(c.ts)
    END AS local_time,
    cn.today_date,
    cn.now_time,
    c.netAmount,
    c.brand,
    c.`group`,
    cn.country
  FROM combined c
  JOIN country_now cn
    ON cn.country = c.country
),

capped AS (
  SELECT
    local_date AS date,
    country,
    brand,
    `group`,
    netAmount
  FROM base
  WHERE local_date BETWEEN DATE_SUB(today_date, INTERVAL 2 DAY) AND today_date
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
