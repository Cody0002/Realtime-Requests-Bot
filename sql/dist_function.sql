WITH deposit_raw AS (
  SELECT
    f.completedAt,
    f.type,
    f.status,
    f.method,
    CAST(f.netAmount AS FLOAT64) AS net_amount,
    f.reqCurrency,
    LEFT(f.reqCurrency, 2) AS country
  FROM
    `kz-dp-prod.kz_pg_to_bq_realtime.ext_funding_tx` AS f
  WHERE
    f.type = 'deposit'
    AND f.status = 'completed'
    -- Range: [target - 8h] (start of UTC+8) to [target + 1d + 6h] (end of UTC-6)
    AND f.insertedAt >= TIMESTAMP_SUB(TIMESTAMP(@target_date), INTERVAL 8 HOUR)
    AND f.insertedAt <  TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP(@target_date), INTERVAL 1 DAY), INTERVAL 6 HOUR)
    AND (@selected_country IS NULL OR LEFT(f.reqCurrency, 2) = @selected_country)
    AND DATE(DATETIME(f.insertedAt, CASE
      WHEN f.reqCurrency = 'BDT' THEN '+06:00' -- UTC+6
      WHEN f.reqCurrency = 'PKR' THEN '+05:00' -- UTC+5
      WHEN f.reqCurrency = 'PHP' THEN '+08:00' -- UTC+8
      WHEN f.reqCurrency = 'THB' THEN '+07:00' -- UTC+7
      WHEN f.reqCurrency = 'BRL' THEN '-03:00' -- UTC-3
      WHEN f.reqCurrency = 'COP' THEN '-05:00' -- UTC-5
      WHEN LEFT(f.reqCurrency, 2) = 'MX' THEN '-06:00' -- UTC-6
      ELSE NULL END)) = @target_date
  QUALIFY ROW_NUMBER() OVER (PARTITION BY f.id ORDER BY f.updatedAt DESC) = 1
),
normalized AS (
  SELECT
    country,
    COALESCE(method, 'UNKNOWN') AS method,
    reqCurrency AS currency,
    net_amount
  FROM
    deposit_raw
  WHERE
    reqCurrency IS NOT NULL
),
grouped AS (
  SELECT
    country,
    method,
    currency,
    COUNT(*) AS deposit_tnx_count,
    SUM(net_amount) AS total_native,
    AVG(net_amount) AS avg_native
  FROM
    normalized
  GROUP BY
    country,
    method,
    currency
)
SELECT
  country,
  method,
  currency,
  deposit_tnx_count,
  ROUND(total_native, 0) AS total_deposit_amount_native,
  ROUND(avg_native, 0) AS average_deposit_amount_native,
  CONCAT(
    ROUND(
      SAFE_DIVIDE(
        total_native * 100.0,
        SUM(total_native) OVER (PARTITION BY country)
      ),
      2
    ),
    '%'
  ) AS pct_of_country_total_native
FROM
  grouped
ORDER BY
  country,
  total_deposit_amount_native DESC,
  method;
