from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from analysis_utils.db import build_mysql_engine  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a SQL file against MySQL.")
    parser.add_argument("sql_file", help="Path to the SQL file to execute.")
    parser.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    parser.add_argument("--user", default=os.getenv("MYSQL_USER", "root"))
    parser.add_argument("--password", default=os.getenv("MYSQL_PASSWORD"))
    parser.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "midproject1"))
    return parser.parse_args()


def split_sql_statements(sql_text: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []

    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            statement = "\n".join(current).strip()
            if statement:
                statements.append(statement)
            current = []

    tail = "\n".join(current).strip()
    if tail:
        statements.append(tail)

    return statements


def main() -> None:
    args = parse_args()
    sql_path = Path(args.sql_file).resolve()
    sql_text = sql_path.read_text(encoding="utf-8")
    statements = split_sql_statements(sql_text)

    engine = build_mysql_engine(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
    )

    with engine.begin() as connection:
        for index, statement in enumerate(statements, start=1):
            print(f"Running statement {index}/{len(statements)}...")
            connection.exec_driver_sql(statement)

    print(f"Completed SQL file: {sql_path}")


if __name__ == "__main__":
    main()
