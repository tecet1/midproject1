-- ex1 step 4
-- Join behavior logs with application data.
--
-- IMPORTANT:
-- The application table column names still need to be confirmed.
-- Replace the TODO columns below after checking 00_schema_export.sql.
--
-- Expected shape from the project brief:
-- - application id
-- - user id
-- - job id
-- - application datetime

-- TODO 1:
-- Replace these placeholder column names with the real ones from your DB.
-- Example guesses:
--   user_uuid
--   job_id
--   applied_at

WITH application_base AS (
    SELECT
        -- TODO: replace with actual application user id column
        user_uuid AS user_uuid,
        -- TODO: replace with actual application timestamp column
        applied_at AS applied_at
    FROM application
),
session_rollup AS (
    SELECT
        user_uuid,
        session_seq,
        MIN(event_ts) AS session_start_ts,
        MAX(event_ts) AS session_end_ts,
        COUNT(*) AS total_events,
        SUM(event_group = 'job_page') AS job_page_events,
        SUM(event_group = 'company_page') AS company_page_events,
        SUM(event_group = 'search') AS search_events,
        SUM(event_group = 'bookmark') AS bookmark_events,
        SUM(event_group = 'notification') AS notification_events,
        SUM(event_group = 'apply') AS apply_events
    FROM ex1_log_sessionized
    GROUP BY user_uuid, session_seq
),
labeled_sessions AS (
    SELECT
        s.*,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM application_base a
                WHERE a.user_uuid = s.user_uuid
                  AND a.applied_at BETWEEN s.session_start_ts
                                       AND s.session_end_ts + INTERVAL 1 HOUR
            ) THEN 1
            ELSE 0
        END AS is_conversion_session
    FROM session_rollup s
)
SELECT
    is_conversion_session,
    COUNT(*) AS sessions,
    AVG(total_events) AS avg_events_per_session,
    AVG(job_page_events) AS avg_job_page_events,
    AVG(company_page_events) AS avg_company_page_events,
    AVG(search_events) AS avg_search_events,
    AVG(bookmark_events) AS avg_bookmark_events,
    AVG(notification_events) AS avg_notification_events,
    AVG(apply_events) AS avg_apply_events
FROM labeled_sessions
GROUP BY is_conversion_session;

-- Once the TODO columns are fixed, this becomes the key ex1 comparison:
-- conversion sessions vs non-conversion sessions.
