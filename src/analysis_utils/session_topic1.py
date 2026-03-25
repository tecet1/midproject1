from __future__ import annotations

from pathlib import Path

import pandas as pd
import polars as pl
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .url_funnel import extract_url_features

LOG_SCHEMA = {
    "user_uuid": pl.String,
    "URL": pl.String,
    "timestamp": pl.String,
    "date": pl.String,
    "response_code": pl.String,
    "method": pl.String,
}

APPLY_SIGNAL_RE = (
    r"apply|application|resume|bookmark|job_offer|notification|other_jobs|"
    r"^jobs(?:/|$)|^api/jobs(?:/|$)|^search(?:/|$)"
)


def scan_topic1_logs(
    topic_dir: str | Path,
    *,
    row_limit_per_file: int | None = None,
) -> pl.LazyFrame:
    topic_path = Path(topic_dir)
    paths = [topic_path / "log_2022.csv", topic_path / "log_2023.csv"]

    lazy_frames: list[pl.LazyFrame] = []
    for path in paths:
        frame = pl.scan_csv(
                path,
                schema=LOG_SCHEMA,
                ignore_errors=True,
                truncate_ragged_lines=True,
            ).with_columns(pl.lit(path.name).alias("source_file"))
        if row_limit_per_file is not None:
            frame = frame.head(row_limit_per_file)
        lazy_frames.append(frame)

    return pl.concat(lazy_frames)


def prepare_topic1_logs(
    topic_dir: str | Path,
    *,
    row_limit_per_file: int | None = None,
) -> pl.LazyFrame:
    return (
        scan_topic1_logs(topic_dir, row_limit_per_file=row_limit_per_file)
        .rename({"URL": "url"})
        .with_columns(
            pl.col("url").fill_null("").str.strip_chars().alias("url"),
            pl.col("method")
            .fill_null("")
            .str.strip_chars()
            .str.to_uppercase()
            .alias("method"),
            pl.col("response_code")
            .fill_null("")
            .str.strip_chars()
            .alias("response_code"),
            pl.col("timestamp")
            .str.strptime(
                pl.Datetime("us"),
                format="%Y-%m-%d %H:%M:%S%.f UTC",
                strict=False,
            )
            .alias("event_ts"),
        )
        .filter(pl.col("user_uuid").is_not_null() & pl.col("event_ts").is_not_null())
        .with_columns(
            pl.col("event_ts").dt.date().alias("event_date"),
            pl.col("response_code").cast(pl.Int32, strict=False).alias("response_code_num"),
            pl.col("url").str.to_lowercase().alias("url_lower"),
        )
        .with_columns(
            pl.col("url_lower")
            .str.contains(APPLY_SIGNAL_RE, literal=False)
            .fill_null(False)
            .alias("has_apply_signal")
        )
        .select(
            "user_uuid",
            "url",
            "url_lower",
            "event_ts",
            "event_date",
            "response_code",
            "response_code_num",
            "method",
            "source_file",
            "has_apply_signal",
        )
    )


def attach_session_ids(
    logs_lf: pl.LazyFrame,
    *,
    gap_minutes: int = 30,
    split_on_day_change: bool = False,
) -> pl.LazyFrame:
    previous_date_expr = pl.col("event_date").shift(1).over("user_uuid")
    previous_ts_expr = pl.col("event_ts").shift(1).over("user_uuid")

    new_session_expr = (
        pl.when(previous_ts_expr.is_null())
        .then(pl.lit(1))
        .when((pl.col("event_ts") - previous_ts_expr) > pl.duration(minutes=gap_minutes))
        .then(pl.lit(1))
        .when(
            pl.lit(split_on_day_change)
            & previous_date_expr.is_not_null()
            & (pl.col("event_date") != previous_date_expr)
        )
        .then(pl.lit(1))
        .otherwise(pl.lit(0))
        .cast(pl.Int64)
    )

    return (
        logs_lf.sort(["user_uuid", "event_ts"])
        .with_columns(
            previous_ts_expr.alias("prev_event_ts"),
            previous_date_expr.alias("prev_event_date"),
        )
        .with_columns(new_session_expr.alias("is_new_session"))
        .with_columns(pl.col("is_new_session").cum_sum().over("user_uuid").alias("session_seq"))
        .with_columns(
            pl.concat_str(
                [
                    pl.col("user_uuid"),
                    pl.lit("_"),
                    pl.col("session_seq").cast(pl.String),
                ]
            ).alias("session_id")
        )
        .drop("prev_event_ts", "prev_event_date")
    )


def build_session_table(sessionized_lf: pl.LazyFrame) -> pl.LazyFrame:
    return (
        sessionized_lf.group_by(["user_uuid", "session_id"])
        .agg(
            pl.col("event_ts").min().alias("session_start"),
            pl.col("event_ts").max().alias("session_end"),
            pl.col("event_date").min().alias("session_date"),
            pl.len().alias("event_count"),
            pl.col("url").n_unique().alias("unique_url_count"),
            pl.col("has_apply_signal").sum().alias("apply_event_count"),
            pl.col("has_apply_signal").any().alias("has_apply_signal"),
        )
        .with_columns(
            (
                (pl.col("session_end") - pl.col("session_start")).dt.total_seconds() / 60
            ).fill_null(0.0).alias("session_duration_min")
        )
        .sort(["session_start", "user_uuid"])
    )


def summarize_session_strategy(session_table_lf: pl.LazyFrame, label: str) -> pl.DataFrame:
    return (
        session_table_lf.select(
            pl.lit(label).alias("session_strategy"),
            pl.len().alias("session_count"),
            pl.col("user_uuid").n_unique().alias("user_count"),
            pl.col("has_apply_signal").sum().alias("apply_session_count"),
            pl.col("event_count").mean().alias("avg_events_per_session"),
            pl.col("session_duration_min").median().alias("median_duration_min"),
        )
        .collect()
    )


def build_apply_session_url_summary(sessionized_lf: pl.LazyFrame) -> pd.DataFrame:
    apply_urls = (
        sessionized_lf.filter(pl.col("has_apply_signal"))
        .group_by("url")
        .agg(
            pl.len().alias("event_count"),
            pl.col("session_id").n_unique().alias("session_count"),
            pl.col("user_uuid").n_unique().alias("user_count"),
        )
        .sort("event_count", descending=True)
        .collect()
        .to_pandas()
    )

    meta = apply_urls["url"].map(extract_url_features).apply(pd.Series)
    return pd.concat([apply_urls, meta], axis=1)


def load_reference_cleaned_url_summary(engine: Engine) -> pd.DataFrame:
    table_check_sql = text(
        """
        SELECT COUNT(*) AS table_exists
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
          AND table_name = 'cleaned_url_summary'
        """
    )

    with engine.connect() as connection:
        table_exists = connection.execute(table_check_sql).scalar_one()

    if table_exists:
        reference_sql = "SELECT * FROM cleaned_url_summary"
    else:
        reference_sql = """
            SELECT
                LOWER(TRIM(url)) AS url,
                SUM(total_count) AS total_requests,
                SUM(CASE WHEN CAST(response_code AS UNSIGNED) BETWEEN 100 AND 199 THEN total_count ELSE 0 END) AS info_cnt,
                SUM(CASE WHEN CAST(response_code AS UNSIGNED) BETWEEN 200 AND 299 THEN total_count ELSE 0 END) AS success_cnt,
                SUM(CASE WHEN CAST(response_code AS UNSIGNED) BETWEEN 300 AND 399 THEN total_count ELSE 0 END) AS redirect_cnt,
                SUM(CASE WHEN CAST(response_code AS UNSIGNED) BETWEEN 400 AND 499 THEN total_count ELSE 0 END) AS client_error_cnt,
                SUM(CASE WHEN CAST(response_code AS UNSIGNED) >= 500 THEN total_count ELSE 0 END) AS server_error_cnt,
                ROUND(SUM(CASE WHEN CAST(response_code AS UNSIGNED) BETWEEN 200 AND 299 THEN total_count ELSE 0 END) / SUM(total_count) * 100, 2) AS success_rate,
                ROUND(SUM(CASE WHEN CAST(response_code AS UNSIGNED) BETWEEN 300 AND 399 THEN total_count ELSE 0 END) / SUM(total_count) * 100, 2) AS redirect_rate,
                ROUND(SUM(CASE WHEN CAST(response_code AS UNSIGNED) BETWEEN 400 AND 499 THEN total_count ELSE 0 END) / SUM(total_count) * 100, 2) AS client_error_rate,
                ROUND(SUM(CASE WHEN CAST(response_code AS UNSIGNED) >= 500 THEN total_count ELSE 0 END) / SUM(total_count) * 100, 2) AS server_error_rate
            FROM url_summary
            WHERE CAST(response_code AS CHAR) REGEXP '^[1-5][0-9]{2}$'
              AND UPPER(TRIM(method)) IN ('GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS', 'HEAD')
              AND url IS NOT NULL
              AND TRIM(url) <> ''
              AND total_count > 0
            GROUP BY LOWER(TRIM(url))
        """

    frame = pd.read_sql(reference_sql, engine)
    meta = frame["url"].map(extract_url_features).apply(pd.Series)
    return pd.concat([frame, meta], axis=1)


def load_reference_cleaned_url_summary_for_urls(
    engine: Engine,
    urls: list[str] | pd.Series,
) -> pd.DataFrame:
    url_list = [str(url).strip().lower() for url in urls if str(url).strip()]
    if not url_list:
        return pd.DataFrame()

    unique_urls = list(dict.fromkeys(url_list))

    table_check_sql = text(
        """
        SELECT COUNT(*) AS table_exists
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
          AND table_name = 'cleaned_url_summary'
        """
    )

    with engine.begin() as connection:
        table_exists = connection.execute(table_check_sql).scalar_one()
        connection.execute(text("DROP TEMPORARY TABLE IF EXISTS tmp_apply_urls"))
        connection.execute(
            text(
                """
                CREATE TEMPORARY TABLE tmp_apply_urls (
                    url TEXT NOT NULL
                )
                """
            )
        )
        connection.execute(
            text("INSERT INTO tmp_apply_urls (url) VALUES (:url)"),
            [{"url": url} for url in unique_urls],
        )

        if table_exists:
            reference_sql = """
                SELECT c.*
                FROM cleaned_url_summary AS c
                INNER JOIN tmp_apply_urls AS t
                    ON LOWER(TRIM(c.url)) = t.url
            """
        else:
            reference_sql = """
                SELECT
                    LOWER(TRIM(u.url)) AS url,
                    SUM(u.total_count) AS total_requests,
                    SUM(CASE WHEN CAST(u.response_code AS UNSIGNED) BETWEEN 100 AND 199 THEN u.total_count ELSE 0 END) AS info_cnt,
                    SUM(CASE WHEN CAST(u.response_code AS UNSIGNED) BETWEEN 200 AND 299 THEN u.total_count ELSE 0 END) AS success_cnt,
                    SUM(CASE WHEN CAST(u.response_code AS UNSIGNED) BETWEEN 300 AND 399 THEN u.total_count ELSE 0 END) AS redirect_cnt,
                    SUM(CASE WHEN CAST(u.response_code AS UNSIGNED) BETWEEN 400 AND 499 THEN u.total_count ELSE 0 END) AS client_error_cnt,
                    SUM(CASE WHEN CAST(u.response_code AS UNSIGNED) >= 500 THEN u.total_count ELSE 0 END) AS server_error_cnt,
                    ROUND(SUM(CASE WHEN CAST(u.response_code AS UNSIGNED) BETWEEN 200 AND 299 THEN u.total_count ELSE 0 END) / SUM(u.total_count) * 100, 2) AS success_rate,
                    ROUND(SUM(CASE WHEN CAST(u.response_code AS UNSIGNED) BETWEEN 300 AND 399 THEN u.total_count ELSE 0 END) / SUM(u.total_count) * 100, 2) AS redirect_rate,
                    ROUND(SUM(CASE WHEN CAST(u.response_code AS UNSIGNED) BETWEEN 400 AND 499 THEN u.total_count ELSE 0 END) / SUM(u.total_count) * 100, 2) AS client_error_rate,
                    ROUND(SUM(CASE WHEN CAST(u.response_code AS UNSIGNED) >= 500 THEN u.total_count ELSE 0 END) / SUM(u.total_count) * 100, 2) AS server_error_rate
                FROM url_summary AS u
                INNER JOIN tmp_apply_urls AS t
                    ON LOWER(TRIM(u.url)) = t.url
                WHERE CAST(u.response_code AS CHAR) REGEXP '^[1-5][0-9]{2}$'
                  AND UPPER(TRIM(u.method)) IN ('GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS', 'HEAD')
                  AND u.url IS NOT NULL
                  AND TRIM(u.url) <> ''
                  AND u.total_count > 0
                GROUP BY LOWER(TRIM(u.url))
            """

        frame = pd.read_sql(reference_sql, connection)

    meta = frame["url"].map(extract_url_features).apply(pd.Series)
    return pd.concat([frame, meta], axis=1)


def compare_apply_urls_to_reference(
    apply_url_summary: pd.DataFrame,
    reference_summary: pd.DataFrame,
) -> pd.DataFrame:
    reference_subset = reference_summary[
        [
            "url",
            "total_requests",
            "route_group",
            "funnel_stage",
            "success_rate",
            "client_error_rate",
            "server_error_rate",
        ]
    ].rename(
        columns={
            "route_group": "reference_route_group",
            "funnel_stage": "reference_funnel_stage",
        }
    )

    compared = apply_url_summary.merge(reference_subset, on="url", how="left")
    compared["event_share_vs_reference"] = (
        compared["event_count"] / compared["total_requests"]
    )
    return compared.sort_values(["event_count", "session_count"], ascending=False)
