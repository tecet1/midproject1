from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from analysis_utils.session_topic1_duckdb import build_topic1_session_artifacts  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build reusable parquet artifacts for topic1 session analysis."
    )
    parser.add_argument(
        "--topic-dir",
        default=str(PROJECT_ROOT / "topic1"),
        help="Directory containing topic1 raw CSV files.",
    )
    parser.add_argument(
        "--artifact-dir",
        default=str(PROJECT_ROOT / "results" / "topic1_ex1"),
        help="Directory where parquet artifacts will be written.",
    )
    parser.add_argument(
        "--gap-minutes",
        type=int,
        default=30,
        help="Inactivity gap used to split sessions.",
    )
    parser.add_argument(
        "--row-limit-per-file",
        type=int,
        default=None,
        help="Optional row cap per log file for a quick dry run.",
    )
    parser.add_argument(
        "--memory-limit",
        default="2GB",
        help="DuckDB memory limit, e.g. 2GB or 3GB.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=2,
        help="DuckDB worker thread count.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    outputs = build_topic1_session_artifacts(
        topic_dir=args.topic_dir,
        artifact_dir=args.artifact_dir,
        gap_minutes=args.gap_minutes,
        row_limit_per_file=args.row_limit_per_file,
        memory_limit=args.memory_limit,
        threads=args.threads,
    )

    print("Artifact build completed.")
    for name, path in outputs.items():
        print(f"{name}: {path}")


if __name__ == "__main__":
    main()
