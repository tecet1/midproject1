from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from analysis_utils.url_funnel import (  # noqa: E402
    build_error_hotspots,
    build_route_group_summary,
    build_source_breakdown,
    build_stage_summary,
    filter_apply_related,
    load_url_summary,
    top_urls,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze apply-related routes from an exported cleaned_url_summary CSV."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to the exported cleaned_url_summary CSV file.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "results" / "job_apply_funnel"),
        help="Directory where CSV outputs will be written.",
    )
    parser.add_argument(
        "--min-requests",
        type=int,
        default=1000,
        help="Minimum request count used for error-hotspot filtering.",
    )
    return parser.parse_args()


def print_section(title: str, frame: pd.DataFrame, rows: int = 10) -> None:
    print(f"\n=== {title} ===")
    if frame.empty:
        print("(no rows)")
        return

    print(frame.head(rows).to_string(index=False))


def main() -> None:
    args = parse_args()

    summary_df = load_url_summary(args.input)
    apply_df = filter_apply_related(summary_df)

    stage_summary = build_stage_summary(summary_df)
    route_group_summary = build_route_group_summary(apply_df)
    error_hotspots = build_error_hotspots(apply_df, min_requests=args.min_requests)
    source_breakdown = build_source_breakdown(apply_df)
    top_apply_urls = top_urls(apply_df, limit=50)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    summary_df.to_csv(output_dir / "url_summary_enriched.csv", index=False)
    apply_df.to_csv(output_dir / "apply_related_urls.csv", index=False)
    stage_summary.to_csv(output_dir / "apply_funnel_stage_summary.csv", index=False)
    route_group_summary.to_csv(output_dir / "apply_route_group_summary.csv", index=False)
    error_hotspots.to_csv(output_dir / "apply_error_hotspots.csv", index=False)
    source_breakdown.to_csv(output_dir / "apply_source_breakdown.csv", index=False)
    top_apply_urls.to_csv(output_dir / "top_apply_urls.csv", index=False)

    total_requests = summary_df["total_requests"].sum()
    apply_requests = apply_df["total_requests"].sum()
    apply_share = (apply_requests / total_requests * 100) if total_requests else 0

    print(f"Input file: {Path(args.input).resolve()}")
    print(f"Total URLs: {len(summary_df):,}")
    print(f"Apply-related URLs: {len(apply_df):,}")
    print(f"Apply-related request share: {apply_share:.2f}%")
    print(f"Saved outputs to: {output_dir.resolve()}")

    print_section("Funnel Stage Summary", stage_summary, rows=20)
    print_section("Route Group Summary", route_group_summary, rows=20)
    print_section("Top Apply URLs", top_apply_urls, rows=20)
    print_section("Apply Error Hotspots", error_hotspots, rows=20)
    print_section("Apply Source Breakdown", source_breakdown, rows=20)


if __name__ == "__main__":
    main()
