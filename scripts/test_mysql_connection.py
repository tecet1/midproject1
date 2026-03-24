from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from analysis_utils.db import build_mysql_engine, test_mysql_connection  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Test SQLAlchemy + PyMySQL connectivity to a MySQL database."
    )
    parser.add_argument("--host", default=None, help="MySQL host")
    parser.add_argument("--port", type=int, default=None, help="MySQL port")
    parser.add_argument("--user", default=None, help="MySQL user")
    parser.add_argument("--password", default=None, help="MySQL password")
    parser.add_argument("--database", default=None, help="MySQL database name")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    engine = build_mysql_engine(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
    )
    result = test_mysql_connection(engine)

    print("MySQL connection succeeded.")
    for key, value in result.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
