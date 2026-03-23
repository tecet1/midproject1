-- ex1 step 2
-- Turn raw events into ordered user sessions.
--
-- Session rule:
-- - same user_uuid
-- - 30 minutes or more gap => new session

DROP TABLE IF EXISTS ex1_log_sessionized;

CREATE TABLE ex1_log_sessionized AS
SELECT
    session_flags.user_uuid,
    session_flags.raw_url,
    session_flags.normalized_url,
    session_flags.event_group,
    session_flags.event_ts,
    session_flags.event_date,
    session_flags.method,
    session_flags.response_code,
    session_flags.prev_event_ts,
    session_flags.prev_event_group,
    session_flags.next_event_group,
    session_flags.is_new_session,
    SUM(session_flags.is_new_session) OVER (
        PARTITION BY session_flags.user_uuid
        ORDER BY session_flags.event_ts, session_flags.normalized_url
        ROWS UNBOUNDED PRECEDING
    ) AS session_seq,
    CONCAT(
        session_flags.user_uuid,
        '-',
        SUM(session_flags.is_new_session) OVER (
            PARTITION BY session_flags.user_uuid
            ORDER BY session_flags.event_ts, session_flags.normalized_url
            ROWS UNBOUNDED PRECEDING
        )
    ) AS session_id
FROM (
    SELECT
        ordered_logs.*,
        CASE
            WHEN ordered_logs.prev_event_ts IS NULL THEN 1
            WHEN TIMESTAMPDIFF(MINUTE, ordered_logs.prev_event_ts, ordered_logs.event_ts) >= 30 THEN 1
            ELSE 0
        END AS is_new_session
    FROM (
        SELECT
            user_uuid,
            raw_url,
            normalized_url,
            event_group,
            event_ts,
            event_date,
            method,
            response_code,
            LAG(event_ts) OVER (
                PARTITION BY user_uuid
                ORDER BY event_ts, normalized_url
            ) AS prev_event_ts,
            LAG(event_group) OVER (
                PARTITION BY user_uuid
                ORDER BY event_ts, normalized_url
            ) AS prev_event_group,
            LEAD(event_group) OVER (
                PARTITION BY user_uuid
                ORDER BY event_ts, normalized_url
            ) AS next_event_group
        FROM ex1_log_base
    ) ordered_logs
) session_flags;

ALTER TABLE ex1_log_sessionized
    ADD INDEX idx_ex1_sessionized_user_ts (user_uuid, event_ts),
    ADD INDEX idx_ex1_sessionized_session_id (session_id),
    ADD INDEX idx_ex1_sessionized_event_group (event_group);

-- Session-level overview
SELECT
    COUNT(*) AS total_events,
    COUNT(DISTINCT user_uuid) AS total_users,
    COUNT(DISTINCT session_id) AS total_sessions
FROM ex1_log_sessionized;

SELECT
    event_group,
    COUNT(*) AS events
FROM ex1_log_sessionized
GROUP BY event_group
ORDER BY events DESC;
