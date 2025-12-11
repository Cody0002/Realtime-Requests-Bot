-- Declare country param (NULL means "all")
-- DECLARE target_country STRING DEFAULT NULL;
-- e.g. SET target_country = 'TH';  -- or leave NULL for all

-- 3-day sliding window: today, -1d, -2d, each capped at "now" in Asia/Bangkok
WITH params AS (
  SELECT
    'Asia/Bangkok' AS tz,
    CURRENT_DATE('Asia/Bangkok') AS today_date,
    CURRENT_TIME('Asia/Bangkok') AS now_time
),
windows AS (
  SELECT
    d AS day_offset,
    DATE_SUB(p.today_date, INTERVAL d DAY) AS date,
    TIMESTAMP(DATETIME(DATE_SUB(p.today_date, INTERVAL d DAY), TIME '00:00:00'), p.tz) AS start_ts,
    TIMESTAMP(DATETIME(DATE_SUB(p.today_date, INTERVAL d DAY), p.now_time), p.tz) AS end_ts
  FROM params p, UNNEST(GENERATE_ARRAY(0,2)) AS d
),
map_country AS (
  SELECT DISTINCT
    UPPER(a.name) AS brand,
    LEFT(f.reqCurrency, 2) AS country
  FROM `kz-dp-prod.kz_pg_to_bq_realtime.ext_funding_tx` f
  LEFT JOIN `kz-dp-prod.kz_pg_to_bq_realtime.account` a ON f.accountId = a.id
  -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  -- OPTIMIZATION 1: Filter countries early here. --
  WHERE @target_country IS NULL OR
    LEFT(f.reqCurrency, 2) = @target_country
  -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
),
-- Registrations (NAR) within each day's partial window up to "now"
view_total AS (
  SELECT
    w.date,
    CONCAT(a.gamePrefix, m.apiIdentifier) AS username,
    a.gamePrefix,
    UPPER(a.`group`) AS `group`,
    UPPER(a.name) AS name,
    mc.country,
    m.id AS member,
    a.id AS account
  FROM `kz-dp-prod.kz_pg_to_bq_realtime.ext_member` AS m
  JOIN `kz-dp-prod.kz_pg_to_bq_realtime.account` AS a
    ON m.accountId = a.id
  LEFT JOIN map_country AS mc
    ON mc.brand = UPPER(a.name)
  CROSS JOIN windows w
  WHERE m.registerAt >= w.start_ts
    AND m.registerAt <  w.end_ts
),
-- All deposits (for global ranking per user), then filter by each window later
total_deposit AS (
  SELECT
    CONCAT(a.gamePrefix, m.apiIdentifier) AS username,
    f.memberId,
    f.completedAt,
    UPPER(a.name)   AS brand,
    UPPER(a.`group`) AS `group`,
    f.id,
    LEFT(f.reqCurrency, 2) AS country,
    f.createdAt
  FROM `kz-dp-prod.kz_pg_to_bq_realtime.ext_funding_tx` f
  LEFT JOIN `kz-dp-prod.kz_pg_to_bq_realtime.ext_member` m ON f.memberId = m.id
  LEFT JOIN `kz-dp-prod.kz_pg_to_bq_realtime.account` a ON f.accountId = a.id
  WHERE f.type = 'deposit'
    AND f.status = 'completed'
    -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
    -- OPTIMIZATION 2: Filter the main deposit table early to speed up the RANK() function. --
    AND (
      @target_country IS NULL OR
      LEFT(f.reqCurrency, 2) = @target_country
    )
    -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  QUALIFY ROW_NUMBER() OVER (PARTITION BY f.id ORDER BY f.updatedAt DESC) = 1
),
-- Rank deposits per user across all time (so FTD/STD/TTD are true 1st/2nd/3rd overall)
ranked_deposit AS (
  SELECT
    td.*,
    RANK() OVER (PARTITION BY username ORDER BY createdAt ASC) AS rank_deposit
  FROM total_deposit td
),
-- For each day-window, keep deposits with completedAt inside that day's partial window
windowed_deposit AS (
  SELECT
    w.date,
    rd.brand,
    rd.`group`,
    rd.country,
    rd.rank_deposit
  FROM ranked_deposit rd
  JOIN windows w
    ON rd.completedAt >= w.start_ts
   AND rd.completedAt <  w.end_ts
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
consolidated_nar AS (
  SELECT
    vt.date,
    vt.`group`,
    vt.name AS brand,
    vt.country,
    COUNT(DISTINCT vt.username) AS NAR
  FROM view_total vt
  GROUP BY vt.date, vt.`group`, vt.name, vt.country
),
brand_total AS (
  SELECT
    brand,
    SUM(NAR) AS total_nar
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
  ON cn.date    = cd.date
 AND cn.brand   = cd.brand
 AND cn.`group` = cd.`group`
 AND cn.country = cd.country
JOIN brand_total bt
  ON cn.brand = bt.brand
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- OPTIMIZATION 3: This filter is no longer needed here. --
-- WHERE @target_country IS NULL OR cn.country = @target_country
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
ORDER BY bt.total_nar DESC, cn.date DESC;