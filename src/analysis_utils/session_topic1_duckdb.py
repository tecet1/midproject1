from __future__ import annotations

import gc
from pathlib import Path

import duckdb
import pandas as pd

from .url_funnel import extract_url_features

APPLY_SIGNAL_PATTERN = (
    "apply|application|resume|bookmark|job_offer|notification|other_jobs|"
    "^jobs(/|$)|^api/jobs(/|$)|^search(/|$)"
)


def _topic1_csv_union_sql(topic_dir: str | Path, row_limit_per_file: int | None = None) -> str:
    topic_path = Path(topic_dir)
    parts: list[str] = []

    for file_name in ("log_2022.csv", "log_2023.csv"):
        path = topic_path / file_name
        limit_sql = f"LIMIT {int(row_limit_per_file)}" if row_limit_per_file else ""
        parts.append(
            f"""
            SELECT *
            FROM (
                SELECT
                    user_uuid,
                    URL,
                    timestamp,
                    date,
                    response_code,
                    method,
                    '{file_name}' AS source_file
                FROM read_csv(
                    '{path.as_posix()}',
                    columns={{
                        'user_uuid': 'VARCHAR',
                        'URL': 'VARCHAR',
                        'timestamp': 'VARCHAR',
                        'date': 'VARCHAR',
                        'response_code': 'VARCHAR',
                        'method': 'VARCHAR'
                    }},
                    header=true,
                    ignore_errors=true,
                    null_padding=true,
                    quote='"',
                    escape='"',
                    parallel=false
                )
                {limit_sql}
            )
            """
        )

    return "\nUNION ALL\n".join(parts)


def _base_session_cte_sql(
    topic_dir: str | Path,
    *,
    gap_minutes: int = 30,
    row_limit_per_file: int | None = None,
) -> str:
    raw_logs_sql = _topic1_csv_union_sql(topic_dir, row_limit_per_file=row_limit_per_file)

    return f"""
    WITH raw_logs AS (
        {raw_logs_sql}
    ),
    parsed AS (
        SELECT
            user_uuid,
            URL AS url,
            lower(trim(URL)) AS url_lower,
            try_strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f UTC') AS event_ts,
            date,
            response_code,
            upper(trim(method)) AS method,
            source_file
        FROM raw_logs
        WHERE user_uuid IS NOT NULL
          AND URL IS NOT NULL
          AND trim(URL) <> ''
    ),
    filtered AS (
        SELECT
            *,
            CASE
                WHEN regexp_matches(url_lower, '{APPLY_SIGNAL_PATTERN}') THEN 1
                ELSE 0
            END AS has_apply_signal
        FROM parsed
        WHERE event_ts IS NOT NULL
    ),
    flagged AS (
        SELECT
            *,
            CASE
                WHEN lag(event_ts) OVER (PARTITION BY user_uuid ORDER BY event_ts) IS NULL THEN 1
                WHEN datediff('minute', lag(event_ts) OVER (PARTITION BY user_uuid ORDER BY event_ts), event_ts) > {int(gap_minutes)} THEN 1
                ELSE 0
            END AS is_new_session
        FROM filtered
    ),
    sessionized AS (
        SELECT
            *,
            sum(is_new_session) OVER (
                PARTITION BY user_uuid
                ORDER BY event_ts
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS session_seq
        FROM flagged
    ),
    session_flags AS (
        SELECT
            *,
            concat(user_uuid, '_', cast(session_seq AS VARCHAR)) AS session_id,
            max(has_apply_signal) OVER (PARTITION BY user_uuid, session_seq) AS session_has_apply_signal
        FROM sessionized
    )
    """


def build_topic1_session_artifacts(
    topic_dir: str | Path,
    artifact_dir: str | Path,
    *,
    gap_minutes: int = 30,
    row_limit_per_file: int | None = None,
    memory_limit: str = "2GB",
    threads: int = 2,
) -> dict[str, Path]:
    artifact_path = Path(artifact_dir)
    artifact_path.mkdir(parents=True, exist_ok=True)
    temp_path = artifact_path / "duckdb_tmp"
    temp_path.mkdir(parents=True, exist_ok=True)

    session_summary_path = artifact_path / "session_summary.parquet"
    apply_session_events_path = artifact_path / "apply_session_events.parquet"
    apply_session_url_summary_path = artifact_path / "apply_session_url_summary.parquet"

    cte_sql = _base_session_cte_sql(
        topic_dir,
        gap_minutes=gap_minutes,
        row_limit_per_file=row_limit_per_file,
    )

    connection = duckdb.connect()
    try:
        connection.execute(f"PRAGMA memory_limit='{memory_limit}'")
        connection.execute(f"PRAGMA threads={int(threads)}")
        connection.execute(f"PRAGMA temp_directory='{temp_path.as_posix()}'")
        connection.execute("PRAGMA preserve_insertion_order=false")

        connection.execute(
            f"""
            COPY (
                {cte_sql}
                SELECT
                    user_uuid,
                    session_id,
                    min(event_ts) AS session_start,
                    max(event_ts) AS session_end,
                    cast(min(event_ts) AS DATE) AS session_date,
                    count(*) AS event_count,
                    count(DISTINCT url) AS unique_url_count,
                    sum(has_apply_signal) AS apply_event_count,
                    max(session_has_apply_signal) AS has_apply_signal,
                    datediff('second', min(event_ts), max(event_ts)) / 60.0 AS session_duration_min
                FROM session_flags
                GROUP BY 1, 2
            ) TO '{session_summary_path.as_posix()}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )

        connection.execute(
            f"""
            COPY (
                {cte_sql}
                SELECT
                    user_uuid,
                    session_id,
                    event_ts,
                    url,
                    url_lower,
                    response_code,
                    method,
                    source_file
                FROM session_flags
                WHERE session_has_apply_signal = 1
            ) TO '{apply_session_events_path.as_posix()}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )

        connection.execute(
            f"""
            COPY (
                {cte_sql}
                SELECT
                    url,
                    count(*) AS event_count,
                    count(DISTINCT session_id) AS session_count,
                    count(DISTINCT user_uuid) AS user_count
                FROM session_flags
                WHERE session_has_apply_signal = 1
                GROUP BY 1
                ORDER BY event_count DESC
            ) TO '{apply_session_url_summary_path.as_posix()}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
    finally:
        connection.close()
        gc.collect()

    return {
        "session_summary": session_summary_path,
        "apply_session_events": apply_session_events_path,
        "apply_session_url_summary": apply_session_url_summary_path,
    }


def load_session_summary(session_summary_path: str | Path) -> pd.DataFrame:
    return duckdb.sql(
        f"SELECT * FROM read_parquet('{Path(session_summary_path).as_posix()}')"
    ).df()


def load_apply_session_url_summary(apply_session_url_summary_path: str | Path) -> pd.DataFrame:
    frame = duckdb.sql(
        f"SELECT * FROM read_parquet('{Path(apply_session_url_summary_path).as_posix()}')"
    ).df()
    meta = frame["url"].map(extract_url_features).apply(pd.Series)
    enriched = pd.concat([frame, meta], axis=1)
    return enriched.sort_values(["event_count", "session_count"], ascending=False)
