from __future__ import annotations

import argparse
import gc
import os
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from analysis_utils.db import build_mysql_engine  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export topic1_log_stage into monthly parquet files."
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "results" / "topic1_monthly_parquet"),
        help="Directory where monthly parquet files will be written.",
    )
    parser.add_argument(
        "--table-name",
        default="topic1_log_stage",
        help="MySQL staging table name.",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=200_000,
        help="Rows per chunk fetched from MySQL.",
    )
    parser.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    parser.add_argument("--user", default=os.getenv("MYSQL_USER", "root"))
    parser.add_argument("--password", default=os.getenv("MYSQL_PASSWORD"))
    parser.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "midproject1"))
    return parser.parse_args()


def month_slug(month_value: str) -> str:
    return month_value[:7].replace("-", "_")


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    engine = build_mysql_engine(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
    )

    month_query = text(
        f"""
        SELECT DISTINCT CAST(event_month AS CHAR) AS event_month
        FROM {args.table_name}
        ORDER BY event_month
        """
    )
    months = pd.read_sql(month_query, engine)["event_month"].tolist()

    for month_value in months:
        month_file = output_dir / f"topic1_stage_{month_slug(month_value)}.parquet"
        if month_file.exists():
            month_file.unlink()

        sql = text(
            f"""
            SELECT
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
            FROM {args.table_name}
            WHERE event_month = :event_month
            ORDER BY user_uuid, event_ts
            """
        )

        writer: pq.ParquetWriter | None = None
        row_count = 0

        with engine.connect() as connection:
            for chunk in pd.read_sql(
                sql,
                connection,
                params={"event_month": month_value},
                chunksize=args.chunksize,
            ):
                table = pa.Table.from_pandas(chunk, preserve_index=False)
                if writer is None:
                    writer = pq.ParquetWriter(
                        month_file,
                        table.schema,
                        compression="zstd",
                    )
                writer.write_table(table)
                row_count += len(chunk)
                del chunk, table
                gc.collect()

        if writer is not None:
            writer.close()

        print(f"{month_file.name}: {row_count} rows")


if __name__ == "__main__":
    main()
