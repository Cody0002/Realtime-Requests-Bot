-- pmh_week_function.sql
-- Params:
--   @as_of_date DATE          -- typically the YYYY-MM-DD you pass with the command
--   @selected_country STRING   -- e.g. 'TH' (nullable = all)

DECLARE tz STRING DEFAULT 'Asia/Bangkok';

WITH bounds AS (
  SELECT
    DATE_TRUNC(@as_of_date, WEEK(MONDAY))                   AS cur_start,
    @as_of_date                                             AS cur_end,
    DATE_SUB(DATE_TRUNC(@as_of_date, WEEK(MONDAY)), INTERVAL 7 DAY) AS prev_start,
    DATE_SUB(@as_of_date, INTERVAL 7 DAY)                   AS prev_end
),

base AS (
  SELECT
    f.type,
    LEFT(f.reqCurrency, 2) AS country,
    f.createdAt,
    f.completedAt,
    f.providerKey,
    f.method,
    a.name AS brand_name,
    CASE
      WHEN f.status = 'errors' THEN 'error'
    ELSE f.status END AS status,
    f.netAmount,
    -- DATE(DATETIME(COALESCE(f.completedAt, f.createdAt), tz)) AS local_date
    DATE(DATETIME(f.createdAt, CASE WHEN f.reqCurrency = 'BDT' THEN '+06:00' -- UTC+6
        WHEN f.reqCurrency = 'PKR' THEN '+05:00' -- UTC+5
        WHEN f.reqCurrency = 'PHP' THEN '+08:00' -- UTC+8
        WHEN f.reqCurrency = 'THB' THEN '+07:00' -- UTC+7
        WHEN f.reqCurrency = 'BRL' THEN '-03:00' -- (America/Sao_Paulo is UTC-3)
        WHEN f.reqCurrency = 'COP' THEN '-05:00' -- UTC-5
        WHEN LEFT(f.reqCurrency, 2) = 'MX' THEN '-06:00' -- UTC-6
        -- WHEN f.reqCurrency = 'IDR' THEN '+07:00' -- (Asia/Jakarta is UTC+7)
        ELSE NULL END)) AS local_date
  FROM `kz-dp-prod.kz_pg_to_bq_realtime.ext_funding_tx` AS f
  LEFT JOIN `kz-dp-prod.kz_pg_to_bq_realtime.account`      AS a ON f.accountId = a.id
  WHERE f.type IN ('deposit','withdraw')
    AND f.status IN ('completed','error','timeout', 'errors')
    AND (@selected_country IS NULL OR
         (LEFT(f.reqCurrency, 2) ) = @selected_country)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY f.id ORDER BY f.updatedAt DESC) = 1
),

cur AS (
  SELECT
    'CUR' AS period, b.*
  FROM base b, bounds d
  WHERE b.local_date BETWEEN d.cur_start AND d.cur_end
),

prev AS (
  SELECT
    'PREV' AS period, b.*
  FROM base b, bounds d
  WHERE b.local_date BETWEEN d.prev_start AND d.prev_end
),

all_tx AS (
  SELECT * FROM cur
  UNION ALL
  SELECT * FROM prev
)

SELECT
  period,
  CASE WHEN type='deposit' THEN 'DEPOSIT'
       WHEN type='withdraw' THEN 'WITHDRAWAL'
  END AS tnx_type,
  providerKey,
  method,
  brand_name AS brand,
  status,
  country,
  AVG(TIMESTAMP_DIFF(completedAt, createdAt, SECOND)) AS avg_diff_seconds_transaction,
  COUNT(*) AS total_count,
  COUNTIF(TIMESTAMP_DIFF(completedAt, createdAt, SECOND) < 180) AS transaction_within_180s,
  COUNTIF(TIMESTAMP_DIFF(completedAt, createdAt, SECOND) < 300) AS transaction_within_300s,
  COUNTIF(TIMESTAMP_DIFF(completedAt, createdAt, SECOND) < 900) AS transaction_within_900s
FROM all_tx
GROUP BY period, tnx_type, providerKey, method, brand, status, country
ORDER BY period, country, brand;
