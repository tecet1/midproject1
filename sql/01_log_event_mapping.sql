-- ex1 step 1
-- Profile raw URLs first, then materialize a cleaned event table.
--
-- Why this file changed:
-- - A VIEW over 16M+ log rows is too slow for repeated work.
-- - Event grouping should be refined after checking top URLs.
--
-- Recommended use:
-- 1) Run the profiling queries first and inspect the top URLs.
-- 2) Adjust the CASE expression if needed.
-- 3) Run the materialization section to build ex1_log_base as a table.

-- --------------------------------------------------------------------
-- A. Raw URL profiling
-- --------------------------------------------------------------------

-- Top normalized URLs by frequency.
-- Start here and inspect whether the event taxonomy matches the real data.
SELECT
    url_no_query,
    COUNT(*) AS events,
    COUNT(DISTINCT user_uuid) AS users
FROM (
    SELECT
        user_uuid,
        LOWER(TRIM(SUBSTRING_INDEX(URL, '?', 1))) AS url_no_query
    FROM log_2022
    WHERE user_uuid IS NOT NULL
      AND URL IS NOT NULL
      AND response_code = '200'

    UNION ALL

    SELECT
        user_uuid,
        LOWER(TRIM(SUBSTRING_INDEX(URL, '?', 1))) AS url_no_query
    FROM log_2023
    WHERE user_uuid IS NOT NULL
      AND URL IS NOT NULL
      AND response_code = '200'
) raw_urls
GROUP BY url_no_query
ORDER BY events DESC
LIMIT 200;

-- Optional helper: inspect only paths likely related to job conversion.
SELECT
    url_no_query,
    COUNT(*) AS events
FROM (
    SELECT LOWER(TRIM(SUBSTRING_INDEX(URL, '?', 1))) AS url_no_query
    FROM log_2022
    WHERE URL IS NOT NULL
      AND response_code = '200'

    UNION ALL

    SELECT LOWER(TRIM(SUBSTRING_INDEX(URL, '?', 1))) AS url_no_query
    FROM log_2023
    WHERE URL IS NOT NULL
      AND response_code = '200'
) candidate_urls
WHERE url_no_query REGEXP 'jobs|companies|bookmark|apply|application|search|suggest|notification'
GROUP BY url_no_query
ORDER BY events DESC
LIMIT 200;

-- --------------------------------------------------------------------
-- B. Materialize the cleaned event table
-- --------------------------------------------------------------------

DROP TABLE IF EXISTS ex1_log_base;

CREATE TABLE ex1_log_base AS
SELECT
    parsed_logs.user_uuid,
    parsed_logs.raw_url,
    parsed_logs.url_no_query,
    parsed_logs.event_ts,
    parsed_logs.event_date,
    parsed_logs.response_code,
    parsed_logs.method,
    REGEXP_REPLACE(
        REGEXP_REPLACE(parsed_logs.url_no_query, '[0-9]+', '{id}'),
        '[0-9a-f]{8}-[0-9a-f-]{27,}',
        '{uuid}'
    ) AS normalized_url,
    CASE
        -- Put more specific conversion-like actions first.
        WHEN parsed_logs.url_no_query REGEXP 'apply|application|applications' THEN 'apply'
        WHEN parsed_logs.url_no_query REGEXP 'bookmark|bookmarks' THEN 'bookmark'
        WHEN parsed_logs.url_no_query REGEXP 'notification|notifications' THEN 'notification'
        WHEN parsed_logs.url_no_query REGEXP 'search|(^|/)suggest($|/)' THEN 'search'
        WHEN parsed_logs.url_no_query REGEXP '(^|/)companies(/|$)' THEN 'company_page'
        WHEN parsed_logs.url_no_query REGEXP '(^|/)jobs(/|$)' THEN 'job_page'
        WHEN parsed_logs.url_no_query REGEXP 'resume|profile|users' THEN 'profile'
        WHEN parsed_logs.url_no_query REGEXP '^/?$|^home$|^main$' THEN 'home'
        WHEN parsed_logs.url_no_query REGEXP 'login|signup|register' THEN 'auth'
        ELSE 'other'
    END AS event_group
FROM (
    SELECT
        raw_logs.user_uuid,
        raw_logs.URL AS raw_url,
        LOWER(TRIM(SUBSTRING_INDEX(raw_logs.URL, '?', 1))) AS url_no_query,
        CASE
            WHEN raw_logs.raw_timestamp LIKE '%.% UTC' THEN
                STR_TO_DATE(REPLACE(raw_logs.raw_timestamp, ' UTC', ''), '%Y-%m-%d %H:%i:%s.%f')
            WHEN raw_logs.raw_timestamp LIKE '% UTC' THEN
                STR_TO_DATE(REPLACE(raw_logs.raw_timestamp, ' UTC', ''), '%Y-%m-%d %H:%i:%s')
            ELSE
                NULL
        END AS event_ts,
        STR_TO_DATE(raw_logs.raw_date, '%Y-%m-%d') AS event_date,
        CAST(raw_logs.response_code AS UNSIGNED) AS response_code,
        UPPER(raw_logs.method) AS method
    FROM (
        SELECT
            user_uuid,
            URL,
            `timestamp` AS raw_timestamp,
            `date` AS raw_date,
            response_code,
            method
        FROM log_2022

        UNION ALL

        SELECT
            user_uuid,
            URL,
            `timestamp` AS raw_timestamp,
            `date` AS raw_date,
            response_code,
            method
        FROM log_2023
    ) raw_logs
    WHERE raw_logs.user_uuid IS NOT NULL
      AND raw_logs.URL IS NOT NULL
) parsed_logs
WHERE parsed_logs.event_ts IS NOT NULL
  AND parsed_logs.response_code = 200;

ALTER TABLE ex1_log_base
    ADD INDEX idx_ex1_log_base_user_ts (user_uuid, event_ts),
    ADD INDEX idx_ex1_log_base_event_group (event_group),
    ADD INDEX idx_ex1_log_base_event_date (event_date);

-- --------------------------------------------------------------------
-- C. Validation after materialization
-- --------------------------------------------------------------------

SELECT COUNT(*) AS total_events
FROM ex1_log_base;

SELECT event_group, COUNT(*) AS events
FROM ex1_log_base
GROUP BY event_group
ORDER BY events DESC;

SELECT normalized_url, COUNT(*) AS events
FROM ex1_log_base
GROUP BY normalized_url
ORDER BY events DESC
LIMIT 50;
