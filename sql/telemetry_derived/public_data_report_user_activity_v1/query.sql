WITH sample AS (
  SELECT
    submission_date,
    days_since_seen,
    country,
    subsession_hours_sum,
    days_seen_bits,
    days_created_profile_bits,
    client_id,
    app_version
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_last_seen`
  WHERE
    DATE(submission_timestamp) > DATE_SUB(@submission_date, INTERVAL 7 DAY)
),
countries AS (
  SELECT
    code,
    name
  FROM
    `moz-fx-data-shared-prod.static.country_names_v1`
  WHERE
    name IN ('Brazil',
         'China',
         'France',
         'Germany',
         'India',
         'Indonesia',
         'Italy',
         'Poland',
         'Russia',
         'United States')
),
mau AS (
  SELECT
    submission_date,
    'Worldwide' AS country_name,
    count(*) AS MAU
  FROM
    sample
  WHERE
    submission_date = @submission_date
    AND days_since_seen < 28
  GROUP BY
    submission_date
  UNION ALL
  SELECT
    submission_date,
    cn.name AS country_name,
    count(*) AS MAU
  FROM
    sample
  LEFT JOIN
    countries AS cn
  ON
    cn.code = country
  WHERE
    submission_date = @submission_date
    AND days_since_seen < 28
  GROUP BY
    submission_date, country_name
),
daily_usage AS (
  SELECT
    'Worldwide' AS country_name,
     avg(subsession_hours_sum) AS avg_hours_usage_daily,
     DATE_ADD(submission_date, INTERVAL EXTRACT(dayofweek FROM submission_date)-1 DAY) AS submission_date
  FROM
    sample
  WHERE
    days_since_seen = 0
    AND subsession_hours_sum < 24 --remove outliers
  GROUP BY
    DATE_ADD(submission_date, INTERVAL extract(dayofweek FROM submission_date)-1 DAY)
  UNION ALL
  SELECT
    cn.name as country_name,
    avg(subsession_hours_sum) AS avg_hours_usage_daily,
    DATE_ADD(submission_date, INTERVAL extract(dayofweek FROM submission_date)-1 DAY) AS submission_date
  FROM
    sample
  LEFT JOIN
    countries AS cn
  ON
    cn.code = country
  WHERE
    days_since_seen = 0
    AND subsession_hours_sum < 24 --remove outliers
    GROUP BY
      name,
      DATE_ADD(submission_date, INTERVAL extract(dayofweek FROM submission_date)-1 DAY)
),
intensity AS (
  SELECT
    submission_date,
    'Worldwide' AS country_name,
    SAFE_DIVIDE(SUM(`moz-fx-data-shared-prod.udf.bitcount_lowest_7`(days_seen_bits)), count(*)) AS intensity
  FROM
    sample
  WHERE
    submission_date = @submission_date
    AND days_since_seen < 7
  GROUP BY
    submission_date
  UNION ALL
  SELECT
    submission_date,
    cn.name AS country_name,
    SAFE_DIVIDE(SUM(`moz-fx-data-shared-prod.udf.bitcount_lowest_7`(days_seen_bits)), count(*)) AS intensity
  FROM
    sample
  LEFT JOIN
    countries AS cn
  ON
    cn.code = country
  WHERE
    submission_date = @submission_date
    AND days_since_seen < 7
  GROUP BY
    submission_date,
    country_name
),
new_profile_rate AS (
  SELECT
    'Worldwide' AS country_name,
    100 * countif(`moz-fx-data-shared-prod.udf.pos_of_trailing_set_bit`(days_created_profile_bits)<7) / -- new profiles
      countif(`moz-fx-data-shared-prod.udf.pos_of_trailing_set_bit`(days_seen_bits)<7) AS new_profile_rate, -- active profiles
     submission_date
  FROM
    sample
  WHERE
    submission_date = @submission_date
  GROUP BY
    submission_date
  UNION ALL
  SELECT
    cn.name AS country_name,
    100 * countif(`moz-fx-data-shared-prod.udf.pos_of_trailing_set_bit`(days_created_profile_bits)<7) / -- new profiles
      countif(`moz-fx-data-shared-prod.udf.pos_of_trailing_set_bit`(days_seen_bits)<7) AS new_profile_rate, -- active profiles
     submission_date
  FROM
    sample
  LEFT JOIN
    countries AS cn
  ON
    cn.code = country
  WHERE
    submission_date = @submission_date
  GROUP BY
    submission_date,
    country_name
),
active_clients_weekly as (
  SELECT
    country,
    client_id,
    split(app_version, '.')[offset(0)] as major_version,
    date_sub(submission_date, interval days_since_seen DAY) as last_day_seen,
    submission_date
  FROM
    sample
  WHERE
    submission_date = @submission_date
    AND days_since_seen < 7
),
latest_releases AS (
  SELECT
    MAX(SPLIT(build.target.version, '.')[OFFSET(0)]) AS latest_major_version,
    DATE(build.build.date) AS day
  FROM
    `moz-fx-data-shared-prod.telemetry.buildhub2`
  WHERE
    build.target.channel ='release'
    AND DATE(build.build.date) >= DATE_SUB(@submission_date, INTERVAL 60 DAY)
  GROUP BY
    day
),
active_clients_with_latest_releases AS (
  SELECT
    client_id,
    country,
    major_version,
    max(latest_major_version) as latest_major_version,
    submission_date
  FROM
    active_clients_weekly
  JOIN
    latest_releases
  ON
    latest_releases.day <= active_clients_weekly.last_day_seen
  WHERE
    client_id IS NOT NULL
  GROUP BY
    client_id,
    country,
    major_version,
    submission_date
),
latest_version_ratio AS (
  SELECT
    'Worldwide' AS country_name,
    countif(major_version=latest_major_version) / count(*) as latest_version_ratio,
    submission_date
  FROM
    active_clients_with_latest_releases
  GROUP BY
    submission_date
  UNION ALL
  SELECT
    cn.name AS country_name,
    countif(major_version=latest_major_version) / count(*) as latest_version_ratio,
    submission_date
  FROM
    active_clients_with_latest_releases
  LEFT JOIN
    countries AS cn
  ON
    cn.code = country
  GROUP BY
    country_name,
    submission_date
)
SELECT
  mau.submission_date,
  mau.country_name,
  mau.mau,
  daily_usage.avg_hours_usage_daily,
  intensity.intensity,
  new_profile_rate.new_profile_rate,
  latest_version_ratio.latest_version_ratio
FROM
  mau
JOIN
  daily_usage
ON
  mau.submission_date=daily_usage.submission_date AND mau.country_name=daily_usage.country_name
JOIN
  intensity
ON
  mau.submission_date=intensity.submission_date AND mau.country_name=intensity.country_name
JOIN
  new_profile_rate
ON
  mau.submission_date=new_profile_rate.submission_date AND mau.country_name=new_profile_rate.country_name
JOIN
  latest_version_ratio
ON
  mau.submission_date=latest_version_ratio.submission_date AND mau.country_name=latest_version_ratio.country_name
ORDER BY
  country_name
