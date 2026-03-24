-- Build an endpoint-level summary from the 2022 and 2023 raw logs.
-- Export the final cleaned_url_summary table in DBeaver for Python-side analysis.

DROP TABLE IF EXISTS url_summary;

CREATE TABLE url_summary AS
SELECT
    url,
    response_code,
    method,
    SUM(cnt) AS total_count
FROM (
    SELECT url, response_code, method, COUNT(*) AS cnt
    FROM log_2022
    GROUP BY url, response_code, method

    UNION ALL

    SELECT url, response_code, method, COUNT(*) AS cnt
    FROM log_2023
    GROUP BY url, response_code, method
) AS aggregated_logs
GROUP BY url, response_code, method
ORDER BY total_count DESC;

SELECT COUNT(*) AS url_summary_rows
FROM url_summary;

DROP TABLE IF EXISTS cleaned_url_summary;

CREATE TABLE cleaned_url_summary AS
WITH cleaned_log AS (
    SELECT
        LOWER(TRIM(url)) AS url,
        CAST(response_code AS UNSIGNED) AS response_code,
        UPPER(TRIM(method)) AS method,
        total_count
    FROM url_summary
    WHERE CAST(response_code AS CHAR) REGEXP '^[1-5][0-9]{2}$'
      AND UPPER(TRIM(method)) IN ('GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS', 'HEAD')
      AND url IS NOT NULL
      AND TRIM(url) <> ''
      AND total_count > 0
)
SELECT
    url,
    SUM(total_count) AS total_requests,
    SUM(CASE WHEN response_code BETWEEN 100 AND 199 THEN total_count ELSE 0 END) AS info_cnt,
    SUM(CASE WHEN response_code BETWEEN 200 AND 299 THEN total_count ELSE 0 END) AS success_cnt,
    SUM(CASE WHEN response_code BETWEEN 300 AND 399 THEN total_count ELSE 0 END) AS redirect_cnt,
    SUM(CASE WHEN response_code BETWEEN 400 AND 499 THEN total_count ELSE 0 END) AS client_error_cnt,
    SUM(CASE WHEN response_code >= 500 THEN total_count ELSE 0 END) AS server_error_cnt,
    ROUND(SUM(CASE WHEN response_code BETWEEN 200 AND 299 THEN total_count ELSE 0 END) / SUM(total_count) * 100, 2) AS success_rate,
    ROUND(SUM(CASE WHEN response_code BETWEEN 300 AND 399 THEN total_count ELSE 0 END) / SUM(total_count) * 100, 2) AS redirect_rate,
    ROUND(SUM(CASE WHEN response_code BETWEEN 400 AND 499 THEN total_count ELSE 0 END) / SUM(total_count) * 100, 2) AS client_error_rate,
    ROUND(SUM(CASE WHEN response_code >= 500 THEN total_count ELSE 0 END) / SUM(total_count) * 100, 2) AS server_error_rate
FROM cleaned_log
GROUP BY url
ORDER BY total_requests DESC;

SELECT COUNT(*) AS cleaned_url_summary_rows
FROM cleaned_url_summary;

SELECT *
FROM cleaned_url_summary
ORDER BY total_requests DESC;

-- Optional: a focused export for likely apply-funnel routes.
SELECT *
FROM cleaned_url_summary
WHERE url REGEXP 'apply|application|jobs|job_offer|resume|bookmark|notification|other_jobs'
ORDER BY total_requests DESC;
