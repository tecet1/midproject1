# %%
from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
TOPIC1_DIR = PROJECT_ROOT / "topic1"

if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from analysis_utils.db import build_mysql_engine, test_mysql_connection  # noqa: E402

# %%
# Set MYSQL_PASSWORD in your terminal before opening the notebook, or fill it in
# temporarily for this session only.
MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE", "midproject1"),
}

engine = build_mysql_engine(**MYSQL_CONFIG)
test_mysql_connection(engine)

# %%
with engine.connect() as connection:
    tables = pl.DataFrame(
        connection.exec_driver_sql("SHOW TABLES").fetchall(),
        schema=["table_name"],
        orient="row",
    )

tables

# %%
topic1_files = sorted(TOPIC1_DIR.glob("*.csv"))
file_inventory = pl.DataFrame(
    {
        "file_name": [path.name for path in topic1_files],
        "size_mb": [round(path.stat().st_size / (1024**2), 2) for path in topic1_files],
    }
).sort("size_mb", descending=True)

file_inventory

# %%
application_df = pl.read_csv(TOPIC1_DIR / "Application.csv")
application_df.head()

# %%
application_df.select(
    pl.len().alias("rows"),
    pl.col("user_uuid").n_unique().alias("unique_users"),
    pl.col("job_uuid").n_unique().alias("unique_jobs"),
    pl.col("company_uuid").n_unique().alias("unique_companies"),
)

# %%
# For the large log CSVs, use Polars lazy scanning with permissive parsing.
log_2022_summary = (
    pl.scan_csv(
        TOPIC1_DIR / "log_2022.csv",
        infer_schema=False,
        ignore_errors=True,
        truncate_ragged_lines=True,
    )
    .group_by("response_code")
    .len()
    .sort("len", descending=True)
    .collect()
)

log_2022_summary.head(10)

# %%
# DuckDB is handy when you want SQL over large CSV files without loading them
# fully into memory first.
duckdb.sql(
    f"""
    SELECT date, COUNT(*) AS requests
    FROM read_csv(
        '{(TOPIC1_DIR / "log_2023.csv").as_posix()}',
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
        quote='\"',
        escape='\"',
        parallel=false
    )
    GROUP BY 1
    ORDER BY requests DESC
    LIMIT 20
    """
).pl()
