-- ex1 step 3
-- Behavior-pattern analysis without using application data yet.
--
-- This is useful when:
-- - application column names are not confirmed yet
-- - you want to understand navigation structure first

-- 1) Most common event groups
SELECT
    event_group,
    COUNT(*) AS total_events,
    COUNT(DISTINCT user_uuid) AS users,
    COUNT(DISTINCT CONCAT(user_uuid, '-', session_seq)) AS sessions
FROM ex1_log_sessionized
GROUP BY event_group
ORDER BY total_events DESC;

-- 2) Most common transitions between event groups
SELECT
    prev_event_group,
    event_group AS current_event_group,
    COUNT(*) AS transitions
FROM ex1_log_sessionized
WHERE prev_event_group IS NOT NULL
GROUP BY prev_event_group, current_event_group
ORDER BY transitions DESC
LIMIT 100;

-- 3) Most common 3-step patterns
WITH three_step AS (
    SELECT
        user_uuid,
        session_seq,
        LAG(event_group, 2) OVER (
            PARTITION BY user_uuid, session_seq
            ORDER BY event_ts, normalized_url
        ) AS event_m2,
        LAG(event_group, 1) OVER (
            PARTITION BY user_uuid, session_seq
            ORDER BY event_ts, normalized_url
        ) AS event_m1,
        event_group AS event_0
    FROM ex1_log_sessionized
)
SELECT
    event_m2,
    event_m1,
    event_0,
    COUNT(*) AS pattern_count
FROM three_step
WHERE event_m2 IS NOT NULL
  AND event_m1 IS NOT NULL
GROUP BY event_m2, event_m1, event_0
ORDER BY pattern_count DESC
LIMIT 100;

-- 4) Session composition
SELECT
    session_id,
    COUNT(*) AS events_in_session
FROM ex1_log_sessionized
GROUP BY session_id
ORDER BY events_in_session DESC
LIMIT 20;

-- 5) Event groups often followed by apply-like pages
SELECT
    prev_event_group,
    COUNT(*) AS transitions_to_apply
FROM ex1_log_sessionized
WHERE event_group = 'apply'
  AND prev_event_group IS NOT NULL
GROUP BY prev_event_group
ORDER BY transitions_to_apply DESC;
