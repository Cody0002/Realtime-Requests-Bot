-- Declare country param (NULL means "all")
-- DECLARE target_country STRING DEFAULT NULL;
-- e.g. SET target_country = 'TH';  -- or leave NULL for all

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
windows AS (
  SELECT
    n.country,
    d AS day_offset,
    DATE_SUB(n.today_date, INTERVAL d DAY) AS date,
    TIMESTAMP(DATETIME(DATE_SUB(n.today_date, INTERVAL d DAY), TIME '00:00:00'), n.tz_offset) AS start_ts,
    TIMESTAMP(DATETIME(DATE_SUB(n.today_date, INTERVAL d DAY), n.now_time), n.tz_offset) AS end_ts
  FROM country_now n, UNNEST(GENERATE_ARRAY(0, 2)) AS d
  WHERE @target_country IS NULL OR n.country = @target_country
),
map_country AS (
  SELECT DISTINCT
    UPPER(a.name) AS brand,
    n.country
  FROM `kz-dp-prod.kz_pg_to_bq_realtime.ext_funding_tx` f
  LEFT JOIN `kz-dp-prod.kz_pg_to_bq_realtime.account` a
    ON f.accountId = a.id
  JOIN country_now n
    ON n.country = LEFT(f.reqCurrency, 2)
  WHERE @target_country IS NULL OR n.country = @target_country
),
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
  JOIN map_country AS mc
    ON mc.brand = UPPER(a.name)
  JOIN windows w
    ON w.country = mc.country
  WHERE m.registerAt >= w.start_ts
    AND m.registerAt <  w.end_ts
),
total_deposit AS (
  SELECT
    CONCAT(a.gamePrefix, m.apiIdentifier) AS username,
    f.memberId,
    f.completedAt,
    UPPER(a.name) AS brand,
    UPPER(a.`group`) AS `group`,
    f.id,
    n.country,
    f.createdAt
  FROM `kz-dp-prod.kz_pg_to_bq_realtime.ext_funding_tx` f
  LEFT JOIN `kz-dp-prod.kz_pg_to_bq_realtime.ext_member` m
    ON f.memberId = m.id
  LEFT JOIN `kz-dp-prod.kz_pg_to_bq_realtime.account` a
    ON f.accountId = a.id
  JOIN country_now n
    ON n.country = LEFT(f.reqCurrency, 2)
  WHERE f.type = 'deposit'
    AND f.status = 'completed'
    -- Range wide enough to cover supported local timezones (+8 to -6).
    AND f.insertedAt >= TIMESTAMP_SUB(TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)), INTERVAL 8 HOUR)
    AND f.insertedAt <  TIMESTAMP_ADD(TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)), INTERVAL 6 HOUR)
    AND (@target_country IS NULL OR n.country = @target_country)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY f.id ORDER BY f.updatedAt DESC) = 1
),
ranked_deposit AS (
  SELECT
    td.*,
    RANK() OVER (PARTITION BY username ORDER BY createdAt ASC) AS rank_deposit
  FROM total_deposit td
),
windowed_deposit AS (
  SELECT
    w.date,
    rd.brand,
    rd.`group`,
    rd.country,
    rd.rank_deposit
  FROM ranked_deposit rd
  JOIN windows w
    ON rd.country = w.country
   AND rd.completedAt >= w.start_ts
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
ORDER BY bt.total_nar DESC, cn.date DESC;
