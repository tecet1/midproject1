DROP TABLE IF EXISTS topic1_log_stage;

CREATE TABLE topic1_log_stage (
    row_id BIGINT NOT NULL AUTO_INCREMENT,
    source_table VARCHAR(20) NOT NULL,
    user_uuid VARCHAR(50) NOT NULL,
    url LONGTEXT NOT NULL,
    url_lower LONGTEXT NOT NULL,
    event_ts DATETIME(6) NOT NULL,
    event_date DATE NOT NULL,
    event_month DATE NOT NULL,
    response_code_num SMALLINT NULL,
    response_code_raw VARCHAR(50) NULL,
    method VARCHAR(20) NULL,
    has_apply_signal TINYINT(1) NOT NULL,
    PRIMARY KEY (row_id),
    KEY idx_topic1_month_user_ts (event_month, user_uuid, event_ts),
    KEY idx_topic1_month_apply (event_month, has_apply_signal),
    KEY idx_topic1_user_ts (user_uuid, event_ts)
);

INSERT INTO topic1_log_stage (
    source_table,
    user_uuid,
    url,
    url_lower,
    event_ts,
    event_date,
    event_month,
    response_code_num,
    response_code_raw,
    method,
    has_apply_signal
)
SELECT
    source_table,
    user_uuid,
    url,
    LOWER(url) AS url_lower,
    event_ts,
    DATE(event_ts) AS event_date,
    CAST(DATE_FORMAT(event_ts, '%%Y-%%m-01') AS DATE) AS event_month,
    response_code_num,
    response_code_raw,
    method,
    CASE
        WHEN LOWER(url) REGEXP 'apply|application|resume|bookmark|job_offer|notification|other_jobs|^jobs(/|$)|^api/jobs(/|$)|^search(/|$)' THEN 1
        ELSE 0
    END AS has_apply_signal
FROM (
    SELECT
        'log_2022' AS source_table,
        TRIM(user_uuid) AS user_uuid,
        TRIM(URL) AS url,
        CASE
            WHEN TRIM(timestamp) REGEXP '\\.[0-9]+ UTC$' THEN STR_TO_DATE(timestamp, '%%Y-%%m-%%d %%H:%%i:%%s.%%f UTC')
            WHEN TRIM(timestamp) REGEXP ' UTC$' THEN STR_TO_DATE(timestamp, '%%Y-%%m-%%d %%H:%%i:%%s UTC')
            ELSE NULL
        END AS event_ts,
        CASE
            WHEN TRIM(response_code) REGEXP '^[0-9]{3}$' THEN CAST(TRIM(response_code) AS UNSIGNED)
            ELSE NULL
        END AS response_code_num,
        TRIM(response_code) AS response_code_raw,
        UPPER(TRIM(method)) AS method
    FROM log_2022

    UNION ALL

    SELECT
        'log_2023' AS source_table,
        TRIM(user_uuid) AS user_uuid,
        TRIM(URL) AS url,
        CASE
            WHEN TRIM(timestamp) REGEXP '\\.[0-9]+ UTC$' THEN STR_TO_DATE(timestamp, '%%Y-%%m-%%d %%H:%%i:%%s.%%f UTC')
            WHEN TRIM(timestamp) REGEXP ' UTC$' THEN STR_TO_DATE(timestamp, '%%Y-%%m-%%d %%H:%%i:%%s UTC')
            ELSE NULL
        END AS event_ts,
        CASE
            WHEN TRIM(response_code) REGEXP '^[0-9]{3}$' THEN CAST(TRIM(response_code) AS UNSIGNED)
            ELSE NULL
        END AS response_code_num,
        TRIM(response_code) AS response_code_raw,
        UPPER(TRIM(method)) AS method
    FROM log_2023
) AS unioned_logs
WHERE user_uuid IS NOT NULL
  AND user_uuid <> ''
  AND url IS NOT NULL
  AND url <> ''
  AND event_ts IS NOT NULL;

ANALYZE TABLE topic1_log_stage;

SELECT
    COUNT(*) AS row_count,
    MIN(event_ts) AS min_event_ts,
    MAX(event_ts) AS max_event_ts,
    COUNT(DISTINCT event_month) AS month_count
FROM topic1_log_stage;
