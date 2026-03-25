from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build monthly and overall session artifacts from monthly parquet files."
    )
    parser.add_argument(
        "--input-dir",
        default=str(Path(__file__).resolve().parents[1] / "results" / "topic1_monthly_parquet"),
        help="Directory containing monthly stage parquet files.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path(__file__).resolve().parents[1] / "results" / "topic1_ex1_monthly"),
        help="Directory where monthly session artifacts will be written.",
    )
    parser.add_argument(
        "--gap-minutes",
        type=int,
        default=30,
        help="Inactivity gap used to split sessions.",
    )
    parser.add_argument(
        "--memory-limit",
        default="2GB",
        help="DuckDB memory limit.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=2,
        help="DuckDB thread count.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    monthly_dir = output_dir / "monthly"
    temp_dir = output_dir / "duckdb_tmp"

    monthly_dir.mkdir(parents=True, exist_ok=True)
    temp_dir.mkdir(parents=True, exist_ok=True)

    parquet_files = sorted(input_dir.glob("topic1_stage_*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No monthly parquet files found in {input_dir}")

    connection = duckdb.connect()
    connection.execute(f"PRAGMA memory_limit='{args.memory_limit}'")
    connection.execute(f"PRAGMA threads={int(args.threads)}")
    connection.execute(f"PRAGMA temp_directory='{temp_dir.as_posix()}'")
    connection.execute("PRAGMA preserve_insertion_order=false")

    for parquet_path in parquet_files:
        month_slug = parquet_path.stem.removeprefix("topic1_stage_")
        session_summary_path = monthly_dir / f"session_summary_{month_slug}.parquet"
        apply_events_path = monthly_dir / f"apply_session_events_{month_slug}.parquet"
        apply_url_summary_path = monthly_dir / f"apply_url_summary_{month_slug}.parquet"

        cte_sql = f"""
        WITH month_logs AS (
            SELECT *
            FROM read_parquet('{parquet_path.as_posix()}')
        ),
        flagged AS (
            SELECT
                *,
                CASE
                    WHEN lag(event_ts) OVER (PARTITION BY user_uuid ORDER BY event_ts) IS NULL THEN 1
                    WHEN datediff('minute', lag(event_ts) OVER (PARTITION BY user_uuid ORDER BY event_ts), event_ts) > {int(args.gap_minutes)} THEN 1
                    ELSE 0
                END AS is_new_session
            FROM month_logs
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
                concat(user_uuid, '_', '{month_slug}', '_', CAST(session_seq AS VARCHAR)) AS session_id,
                max(has_apply_signal) OVER (PARTITION BY user_uuid, session_seq) AS session_has_apply_signal
            FROM sessionized
        )
        """

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
                    event_month,
                    url,
                    url_lower,
                    response_code_num,
                    method
                FROM session_flags
                WHERE session_has_apply_signal = 1
            ) TO '{apply_events_path.as_posix()}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )

        connection.execute(
            f"""
            COPY (
                SELECT
                    url,
                    count(*) AS event_count,
                    count(DISTINCT session_id) AS session_count,
                    count(DISTINCT user_uuid) AS user_count
                FROM read_parquet('{apply_events_path.as_posix()}')
                GROUP BY 1
                ORDER BY event_count DESC
            ) TO '{apply_url_summary_path.as_posix()}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )

        print(f"Built monthly artifacts for {month_slug}")

    connection.execute(
        f"""
        COPY (
            SELECT *
            FROM read_parquet('{(monthly_dir / "session_summary_*.parquet").as_posix()}')
        ) TO '{(output_dir / "session_summary_all.parquet").as_posix()}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    connection.execute(
        f"""
        COPY (
            SELECT
                url,
                count(*) AS event_count,
                count(DISTINCT session_id) AS session_count,
                count(DISTINCT user_uuid) AS user_count
            FROM read_parquet('{(monthly_dir / "apply_session_events_*.parquet").as_posix()}')
            GROUP BY 1
            ORDER BY event_count DESC
        ) TO '{(output_dir / "apply_session_url_summary_all.parquet").as_posix()}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    connection.close()
    print(f"Built overall artifacts in {output_dir}")


if __name__ == "__main__":
    main()
