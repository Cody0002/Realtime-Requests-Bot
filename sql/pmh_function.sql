WITH all_transactions AS (
  -- STEP 1: Scan the base table
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
    f.netAmount
  FROM
    `kz-dp-prod.kz_pg_to_bq_realtime.ext_funding_tx` AS f
  LEFT JOIN
    `kz-dp-prod.kz_pg_to_bq_realtime.account` AS a ON f.accountId = a.id
  WHERE
    f.type IN ('deposit', 'withdraw')
    AND f.status IN ('completed', 'error' ,'timeout', 'errors')
    
    --------------------------------------------------------------------------
    -- ✅ PARTITION PRUNING (The Optimization)
    -- We select a window wide enough to cover all your specific Timezones.
    -- Range: [Target - 8h] (start of UTC+8) to [Target + 1d + 6h] (end of UTC-6)
    --------------------------------------------------------------------------
    AND f.insertedAt >= TIMESTAMP_SUB(TIMESTAMP(@target_date), INTERVAL 8 HOUR) 
    AND f.insertedAt <  TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP(@target_date), INTERVAL 1 DAY), INTERVAL 6 HOUR)
    --------------------------------------------------------------------------

    -- 1. Country Filter
    AND (@selected_country IS NULL OR 
         (LEFT(f.reqCurrency, 2)) = @selected_country)
    
    -- 2. Precise Date Filter (This now runs on only ~2 days of data instead of All-Time)
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
)

-- STEP 2: Perform aggregation
SELECT
  CASE 
    WHEN t.type = 'deposit' THEN 'DEPOSIT' 
    WHEN t.type = 'withdraw' THEN 'WITHDRAWAL' 
  END AS tnx_type,
  t.providerKey,
  t.method,
  t.brand_name as brand,
  t.status,
  t.country,
  AVG(TIMESTAMP_DIFF(t.completedAt, t.createdAt, SECOND)) AS avg_diff_seconds_transaction,
  COUNT(*) AS total_count,
  COUNTIF(TIMESTAMP_DIFF(t.completedAt, t.createdAt, SECOND) < 180) AS transaction_within_180s,
  COUNTIF(TIMESTAMP_DIFF(t.completedAt, t.createdAt, SECOND) < 300) AS transaction_within_300s,
  COUNTIF(TIMESTAMP_DIFF(t.completedAt, t.createdAt, SECOND) < 900) AS transaction_within_900s
FROM
  all_transactions AS t
GROUP BY
  tnx_type,
  providerKey,
  method,
  brand_name,
  status,
  country;
