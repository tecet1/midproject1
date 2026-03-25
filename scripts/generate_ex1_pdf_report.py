from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import seaborn as sns
from matplotlib import font_manager, pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from analysis_utils.db import build_mysql_engine  # noqa: E402
from analysis_utils.session_topic1 import (  # noqa: E402
    compare_apply_urls_to_reference,
    load_reference_cleaned_url_summary_for_urls,
)
from analysis_utils.url_funnel import extract_url_features  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a PDF report for EX1 user session analysis."
    )
    parser.add_argument(
        "--artifact-root",
        default=str(PROJECT_ROOT / "results" / "topic1_ex1_monthly"),
        help="Directory containing final monthly pipeline artifacts.",
    )
    parser.add_argument(
        "--output",
        default=str(PROJECT_ROOT / "results" / "reports" / "ex1_user_session_report.pdf"),
        help="Output PDF path.",
    )
    parser.add_argument(
        "--min-event-count",
        type=int,
        default=20,
        help="Minimum event_count used for readable URL-level summary in the report.",
    )
    parser.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    parser.add_argument("--user", default=os.getenv("MYSQL_USER", "root"))
    parser.add_argument("--password", default=os.getenv("MYSQL_PASSWORD"))
    parser.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "midproject1"))
    return parser.parse_args()


def configure_plot_style() -> None:
    malgun_path = Path("C:/Windows/Fonts/malgun.ttf")
    if malgun_path.exists():
        font_manager.fontManager.addfont(str(malgun_path))
        plt.rcParams["font.family"] = "Malgun Gothic"
    else:
        font_names = {font.name for font in font_manager.fontManager.ttflist}
        if "Malgun Gothic" in font_names:
            plt.rcParams["font.family"] = "Malgun Gothic"
    plt.rcParams["axes.unicode_minus"] = False
    sns.set_theme(style="whitegrid")


def load_report_frames(artifact_root: Path, min_event_count: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    session_path = (artifact_root / "session_summary_all.parquet").as_posix()
    apply_path = (artifact_root / "apply_session_url_summary_all.parquet").as_posix()
    monthly_path = (artifact_root / "monthly" / "session_summary_*.parquet").as_posix()

    session_overview = duckdb.sql(
        f"""
        SELECT
            COUNT(*) AS session_count,
            COUNT(DISTINCT user_uuid) AS user_count,
            SUM(has_apply_signal) AS apply_session_count,
            AVG(has_apply_signal) AS apply_session_share,
            AVG(event_count) AS avg_events_per_session,
            MEDIAN(session_duration_min) AS median_duration_min
        FROM read_parquet('{session_path}')
        """
    ).df()

    apply_url_summary = duckdb.sql(
        f"""
        SELECT *
        FROM read_parquet('{apply_path}')
        WHERE event_count >= {int(min_event_count)}
        ORDER BY event_count DESC
        """
    ).df()

    meta = apply_url_summary["url"].map(extract_url_features).apply(pd.Series)
    apply_url_summary = pd.concat([apply_url_summary, meta], axis=1)

    month_summary = duckdb.sql(
        f"""
        SELECT
            regexp_extract(filename, 'session_summary_(\\d{{4}}_\\d{{2}})', 1) AS month_slug,
            COUNT(*) AS session_count,
            SUM(has_apply_signal) AS apply_session_count,
            AVG(event_count) AS avg_events_per_session,
            MEDIAN(session_duration_min) AS median_duration_min
        FROM read_parquet('{monthly_path}', filename=true)
        GROUP BY 1
        ORDER BY 1
        """
    ).df()

    coverage = duckdb.sql(
        f"""
        SELECT
            SUM(CASE WHEN event_count >= {int(min_event_count)} THEN event_count ELSE 0 END) * 1.0 / SUM(event_count) AS kept_event_share,
            SUM(CASE WHEN event_count >= {int(min_event_count)} THEN session_count ELSE 0 END) * 1.0 / SUM(session_count) AS kept_session_share
        FROM read_parquet('{apply_path}')
        """
    ).df()

    return session_overview, apply_url_summary, month_summary, coverage


def build_route_group_summary(apply_url_summary: pd.DataFrame) -> pd.DataFrame:
    return (
        apply_url_summary.groupby("route_group", dropna=False)
        .agg(
            url_count=("url", "nunique"),
            event_count=("event_count", "sum"),
            session_count=("session_count", "sum"),
            user_count=("user_count", "sum"),
        )
        .sort_values(["event_count", "session_count"], ascending=False)
        .reset_index()
    )


def render_text_page(pdf: PdfPages, title: str, lines: list[str]) -> None:
    fig, ax = plt.subplots(figsize=(8.27, 11.69))
    ax.axis("off")
    ax.text(0.03, 0.97, title, fontsize=18, fontweight="bold", va="top")
    ax.text(0.03, 0.92, "\n".join(lines), fontsize=11, va="top", linespacing=1.6)
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def render_table_page(pdf: PdfPages, title: str, frame: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(8.27, 11.69))
    ax.axis("off")
    ax.set_title(title, fontsize=16, fontweight="bold", pad=20)
    table = ax.table(
        cellText=frame.values,
        colLabels=frame.columns,
        loc="center",
        cellLoc="left",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 1.4)
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def render_chart_page(pdf: PdfPages, fig: plt.Figure) -> None:
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    configure_plot_style()

    artifact_root = Path(args.artifact_root)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    session_overview, apply_url_summary, month_summary, coverage = load_report_frames(
        artifact_root,
        min_event_count=args.min_event_count,
    )
    route_group_summary = build_route_group_summary(apply_url_summary)

    engine = build_mysql_engine(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
    )
    reference_summary = load_reference_cleaned_url_summary_for_urls(
        engine,
        apply_url_summary["url"].head(200).tolist(),
    )
    comparison = compare_apply_urls_to_reference(
        apply_url_summary.head(200),
        reference_summary,
    )
    comparison["exists_in_reference"] = comparison["total_requests"].notna()

    overview_row = session_overview.iloc[0]
    coverage_row = coverage.iloc[0]
    top_route = route_group_summary.iloc[0]

    title_lines = [
        "Project Reminder",
        "",
        "The assignment is EX1: analyze user behavior patterns and propose actions that can increase application conversion.",
        "The original four-step analysis direction was:",
        "1. Build session IDs per user",
        "2. Extract apply-related sessions",
        "3. Parse URLs inside those sessions",
        "4. Compare them with the cleaned_url_summary logic",
        "",
        "The direct raw-CSV sessionization path hit timeout limits,",
        "so the pipeline was changed to:",
        "MySQL stage table -> monthly parquet -> monthly session artifacts -> final merged outputs.",
        "This avoids recomputing the same heavy logic and keeps notebook memory usage low.",
    ]

    method_lines = [
        "Methodology and Interpretation Notes",
        "",
        f"- Session rule: create a new session when the gap between two events from the same user exceeds 30 minutes.",
        "- Because sessions were built month by month, a small number of sessions around month boundaries may be split.",
        f"- URL-level detailed interpretation uses only URLs with event_count >= {args.min_event_count}.",
        "- This does not delete raw data. It only trims the long-tail URL list for readable interpretation.",
        f"- With this threshold, the report keeps {coverage_row['kept_event_share']:.1%} of events and {coverage_row['kept_session_share']:.1%} of sessions in the URL-level summary.",
        "- So the threshold should be read as a readability filter, not as a 'minimum applicant count' rule.",
    ]

    key_metrics = pd.DataFrame(
        {
            "metric": [
                "session_count",
                "user_count",
                "apply_session_count",
                "apply_session_share",
                "avg_events_per_session",
                "median_duration_min",
                "filtered_apply_urls",
            ],
            "value": [
                int(overview_row["session_count"]),
                int(overview_row["user_count"]),
                int(overview_row["apply_session_count"]),
                round(float(overview_row["apply_session_share"]), 4),
                round(float(overview_row["avg_events_per_session"]), 2),
                round(float(overview_row["median_duration_min"]), 2),
                len(apply_url_summary),
            ],
        }
    )

    top_route_table = route_group_summary.head(10).copy()
    top_route_table["event_count"] = top_route_table["event_count"].astype(int)
    top_route_table["session_count"] = top_route_table["session_count"].astype(int)

    top_url_table = comparison[
        [
            "url",
            "event_count",
            "session_count",
            "route_group",
            "reference_route_group",
            "total_requests",
        ]
    ].head(12).copy()
    top_url_table["event_count"] = top_url_table["event_count"].astype(int)
    top_url_table["session_count"] = top_url_table["session_count"].astype(int)
    top_url_table["total_requests"] = top_url_table["total_requests"].fillna(0).astype(int)

    insight_lines = [
        "Key Interpretation Points",
        "",
        f"- There are {int(overview_row['session_count']):,} sessions in total, and {int(overview_row['apply_session_count']):,} of them are apply-related.",
        f"- The share of apply-related sessions is {overview_row['apply_session_share']:.1%}.",
        f"- The largest route group is '{top_route['route_group']}', with {int(top_route['event_count']):,} events.",
        "- The top URLs show repeated presence of job detail pages, other_jobs recommendation areas, and template/profile preparation pages.",
        "- This suggests that information discovery, related-job recommendation, and profile readiness are tightly connected to the apply journey.",
        "- A realistic product recommendation is to optimize not only the apply button itself, but also the flow from job detail -> related jobs -> profile/template preparation.",
    ]

    with PdfPages(output_path) as pdf:
        render_text_page(pdf, "EX1 User Session Analysis Report", title_lines)
        render_text_page(pdf, "Methodology", method_lines)
        render_table_page(pdf, "Key Metrics", key_metrics)

        fig, ax = plt.subplots(figsize=(11.69, 8.27))
        sns.lineplot(data=month_summary, x="month_slug", y="apply_session_count", marker="o", ax=ax)
        ax.set_title("Monthly Apply-Related Session Count", fontsize=16, fontweight="bold")
        ax.set_xlabel("month")
        ax.set_ylabel("apply_session_count")
        plt.xticks(rotation=45)
        render_chart_page(pdf, fig)

        fig, ax = plt.subplots(figsize=(11.69, 8.27))
        sns.barplot(data=route_group_summary.head(12), y="route_group", x="event_count", ax=ax)
        ax.set_title("Top Route Groups Inside Apply-Related Sessions", fontsize=16, fontweight="bold")
        ax.set_xlabel("event_count")
        ax.set_ylabel("route_group")
        render_chart_page(pdf, fig)

        render_table_page(pdf, "Top Route Groups", top_route_table)
        render_table_page(pdf, "Top URLs vs Reference Summary", top_url_table)
        render_text_page(pdf, "Interpretation", insight_lines)

    print(f"Report saved to: {output_path}")
    print(f"Generated at: {datetime.now().isoformat(timespec='seconds')}")


if __name__ == "__main__":
    main()
