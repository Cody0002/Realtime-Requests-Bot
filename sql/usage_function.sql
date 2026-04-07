-- Hidden /usage command: last 3 days query usage for current bot identity.
-- Uses billed bytes so the number maps to chargeable query volume.
WITH days AS (
  SELECT day
  FROM UNNEST(
    GENERATE_DATE_ARRAY(
      DATE_SUB(CURRENT_DATE('Asia/Bangkok'), INTERVAL 2 DAY),
      CURRENT_DATE('Asia/Bangkok')
    )
  ) AS day
),
agg AS (
  SELECT
    DATE(creation_time, 'Asia/Bangkok') AS day,
    COUNT(1) AS query_count,
    SUM(COALESCE(total_bytes_billed, 0)) AS total_bytes_billed
  FROM `__PROJECT_ID__`.`region-__BQ_LOCATION__`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
  WHERE job_type = 'QUERY'
    AND state = 'DONE'
    AND user_email = SESSION_USER()
    AND DATE(creation_time, 'Asia/Bangkok')
      BETWEEN DATE_SUB(CURRENT_DATE('Asia/Bangkok'), INTERVAL 2 DAY)
          AND CURRENT_DATE('Asia/Bangkok')
  GROUP BY day
)
SELECT
  d.day AS usage_date,
  COALESCE(a.query_count, 0) AS query_count,
  ROUND(COALESCE(a.total_bytes_billed, 0) / POW(1024, 3), 3) AS total_gb
FROM days d
LEFT JOIN agg a
  ON a.day = d.day
ORDER BY usage_date DESC
LIMIT 3;
