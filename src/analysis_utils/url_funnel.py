from __future__ import annotations

import re
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import pandas as pd

NUMERIC_COLUMNS = [
    "total_requests",
    "info_cnt",
    "success_cnt",
    "redirect_cnt",
    "client_error_cnt",
    "server_error_cnt",
    "success_rate",
    "redirect_rate",
    "client_error_rate",
    "server_error_rate",
]

PAGE_APPLY_RE = re.compile(r"^jobs/(?:id|:id)/apply/step(?P<step>[1-4])$")
API_APPLY_RE = re.compile(r"^api/jobs/(?:id|:id)/apply/step(?P<step>[1-4])$")
JOB_DETAIL_RE = re.compile(r"^jobs/(?:id|:id)/[^/]+$")

APPLY_KEYWORDS = (
    "apply",
    "application",
    "jobs",
    "job_offer",
    "resume",
    "bookmark",
    "notification",
    "other_jobs",
)

FUNNEL_STAGE_ORDER = [
    "job_discovery",
    "job_detail",
    "resume_prep",
    "apply_step_1_page",
    "apply_step_1_api",
    "apply_step_2_page",
    "apply_step_2_api",
    "apply_step_3_page",
    "apply_step_3_api",
    "apply_step_4_page",
    "apply_step_4_api",
    "apply_complete",
]


def load_url_summary(csv_path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    for column in NUMERIC_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)

    if "url" not in df.columns:
        raise ValueError("The input file must contain a 'url' column.")

    meta = df["url"].map(extract_url_features).apply(pd.Series)
    enriched = pd.concat([df, meta], axis=1)
    enriched["is_apply_related"] = enriched["route_group"].ne("other")
    return enriched


def extract_url_features(raw_url: Any) -> dict[str, Any]:
    original = "" if pd.isna(raw_url) else str(raw_url).strip()
    decoded = unquote(original)

    if "://" in decoded:
        parsed = urlparse(decoded)
        path = parsed.path
        query_string = parsed.query
    else:
        path, _, query_string = decoded.partition("?")

    normalized_path = normalize_path(path)
    query = parse_qs(query_string, keep_blank_values=True)
    route_group, funnel_stage = classify_route(normalized_path, query)

    return {
        "normalized_path": normalized_path,
        "query_string": query_string,
        "query_key_count": len(query),
        "has_query_string": bool(query_string),
        "utm_source": first_query_value(query, "utm_source"),
        "utm_medium": first_query_value(query, "utm_medium"),
        "utm_campaign": first_query_value(query, "utm_campaign"),
        "utm_content": first_query_value(query, "utm_content"),
        "route_group": route_group,
        "funnel_stage": funnel_stage,
    }


def normalize_path(path: str) -> str:
    cleaned = path.strip().strip("/")
    cleaned = re.sub(r"/+", "/", cleaned)
    cleaned = cleaned.lower()

    segments = []
    for segment in cleaned.split("/"):
        if not segment:
            continue
        if re.fullmatch(r"\d+", segment):
            segments.append(":id")
        elif re.fullmatch(r"[0-9a-f]{8,}", segment):
            segments.append(":id")
        else:
            segments.append(segment)

    return "/".join(segments)


def first_query_value(query: dict[str, list[str]], key: str) -> str | None:
    values = query.get(key)
    if not values:
        return None

    value = values[0].strip()
    return value or None


def classify_route(
    normalized_path: str, query: dict[str, list[str]]
) -> tuple[str, str | None]:
    if not normalized_path:
        return "other", None

    page_step = PAGE_APPLY_RE.match(normalized_path)
    if page_step:
        step = page_step.group("step")
        return f"apply_page_step_{step}", f"apply_step_{step}_page"

    api_step = API_APPLY_RE.match(normalized_path)
    if api_step:
        step = api_step.group("step")
        return f"apply_api_step_{step}", f"apply_step_{step}_api"

    if normalized_path == "jobs/id/apply/complete":
        return "apply_complete", "apply_complete"

    if JOB_DETAIL_RE.match(normalized_path):
        return "job_detail", "job_detail"

    if normalized_path in {"jobs", "search"} or normalized_path.startswith("api/jobs/job_title"):
        return "job_discovery", "job_discovery"

    if normalized_path in {
        "@user_id/resume",
        "@user_id/resume/step1",
        "@user_id/resume/step2",
        "api/users/id/resume/step1",
        "api/users/id/resume/step2",
    }:
        return "resume_prep", "resume_prep"

    if normalized_path == "api/users/id/experience/form" and first_query_value(query, "type") == "apply":
        return "resume_prep", "resume_prep"

    if "bookmark" in normalized_path:
        return "bookmark", None

    if "job_offer" in normalized_path:
        return "job_offer", None

    if "notification" in normalized_path:
        return "notifications", None

    if "jobs" in normalized_path or any(keyword in normalized_path for keyword in APPLY_KEYWORDS):
        return "apply_related_other", None

    return "other", None


def filter_apply_related(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[df["is_apply_related"]].copy()


def build_stage_summary(df: pd.DataFrame) -> pd.DataFrame:
    stage_df = (
        df.loc[df["funnel_stage"].notna()]
        .groupby(["funnel_stage"], as_index=False)
        .agg(
            total_requests=("total_requests", "sum"),
            success_cnt=("success_cnt", "sum"),
            redirect_cnt=("redirect_cnt", "sum"),
            client_error_cnt=("client_error_cnt", "sum"),
            server_error_cnt=("server_error_cnt", "sum"),
        )
    )

    stage_df["success_rate"] = (stage_df["success_cnt"] / stage_df["total_requests"] * 100).round(2)
    stage_df["client_error_rate"] = (
        stage_df["client_error_cnt"] / stage_df["total_requests"] * 100
    ).round(2)
    stage_df["server_error_rate"] = (
        stage_df["server_error_cnt"] / stage_df["total_requests"] * 100
    ).round(2)

    order_map = {stage: index for index, stage in enumerate(FUNNEL_STAGE_ORDER)}
    stage_df["stage_order"] = stage_df["funnel_stage"].map(order_map)
    stage_df = stage_df.sort_values(["stage_order", "total_requests"], ascending=[True, False])
    return stage_df.drop(columns=["stage_order"])


def build_route_group_summary(df: pd.DataFrame) -> pd.DataFrame:
    group_df = (
        df.groupby(["route_group"], as_index=False)
        .agg(
            url_count=("url", "count"),
            total_requests=("total_requests", "sum"),
            success_cnt=("success_cnt", "sum"),
            client_error_cnt=("client_error_cnt", "sum"),
            server_error_cnt=("server_error_cnt", "sum"),
        )
        .sort_values("total_requests", ascending=False)
    )

    group_df["success_rate"] = (group_df["success_cnt"] / group_df["total_requests"] * 100).round(2)
    group_df["client_error_rate"] = (
        group_df["client_error_cnt"] / group_df["total_requests"] * 100
    ).round(2)
    return group_df


def build_error_hotspots(df: pd.DataFrame, min_requests: int = 1_000) -> pd.DataFrame:
    hotspots = df.loc[df["total_requests"] >= min_requests].copy()
    hotspots["combined_error_rate"] = (
        (hotspots["client_error_cnt"] + hotspots["server_error_cnt"])
        / hotspots["total_requests"]
        * 100
    ).round(2)

    return hotspots.sort_values(
        ["combined_error_rate", "client_error_cnt", "server_error_cnt", "total_requests"],
        ascending=[False, False, False, False],
    )[
        [
            "url",
            "route_group",
            "funnel_stage",
            "total_requests",
            "success_cnt",
            "client_error_cnt",
            "server_error_cnt",
            "combined_error_rate",
        ]
    ]


def build_source_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    source_df = df.loc[df["utm_source"].notna()].copy()
    if source_df.empty:
        return pd.DataFrame(
            columns=[
                "utm_source",
                "utm_medium",
                "utm_campaign",
                "url_count",
                "total_requests",
                "success_cnt",
                "client_error_cnt",
                "server_error_cnt",
            ]
        )

    return (
        source_df.groupby(["utm_source", "utm_medium", "utm_campaign"], dropna=False, as_index=False)
        .agg(
            url_count=("url", "count"),
            total_requests=("total_requests", "sum"),
            success_cnt=("success_cnt", "sum"),
            client_error_cnt=("client_error_cnt", "sum"),
            server_error_cnt=("server_error_cnt", "sum"),
        )
        .sort_values("total_requests", ascending=False)
    )


def top_urls(df: pd.DataFrame, limit: int = 30) -> pd.DataFrame:
    return df.sort_values("total_requests", ascending=False).head(limit).copy()
