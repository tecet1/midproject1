# EX1 Workflow Handoff

Last updated: 2026-03-24

## Goal

This project is focused on EX1:

- Analyze user behavior patterns
- Extract product insights related to the application journey
- Propose actions that may increase application conversion

The working 4-step analysis structure is:

1. Build session IDs per user
2. Extract apply-related sessions
3. Parse URLs inside those sessions
4. Compare the result with the `cleaned_url_summary` logic

## Final Pipeline Chosen

The original raw-CSV-direct sessionization path was too heavy and hit timeouts.

The current stable workflow is:

1. Build a MySQL staging table from `log_2022` + `log_2023`
2. Export the stage table into monthly parquet files
3. Build monthly 30-minute session artifacts
4. Merge monthly outputs into final parquet artifacts
5. Run notebook/report logic on the compact parquet outputs

This is the best current workflow for this machine.

## Important Files

- Stage table SQL:
  - `sql/build_topic1_log_stage.sql`
- SQL runner:
  - `scripts/run_sql_file.py`
- Monthly parquet export:
  - `scripts/export_topic1_stage_monthly_parquet.py`
- Monthly session artifact builder:
  - `scripts/build_topic1_monthly_session_artifacts.py`
- Final notebook:
  - `notebooks/05_ex1_user_session_apply_analysis_monthly_pipeline.ipynb`
- PDF report generator:
  - `scripts/generate_ex1_pdf_report.py`

Core helper modules:

- `src/analysis_utils/db.py`
- `src/analysis_utils/url_funnel.py`
- `src/analysis_utils/session_topic1.py`
- `src/analysis_utils/session_topic1_duckdb.py`

## Current Outputs Already Created

Monthly stage parquet:

- `results/topic1_monthly_parquet/`

Monthly session artifacts:

- `results/topic1_ex1_monthly/monthly/`

Merged final artifacts:

- `results/topic1_ex1_monthly/session_summary_all.parquet`
- `results/topic1_ex1_monthly/apply_session_url_summary_all.parquet`

Generated PDF report:

- `results/reports/ex1_user_session_report.pdf`

## Current Headline Metrics

From the final monthly pipeline:

- Total sessions: `892,359`
- Distinct users: `21,348`
- Apply-related sessions: `675,004`
- Apply-related session share: `75.64%`
- Average events per session: `18.61`
- Median session duration: `0.88` minutes

Examples of top URLs inside apply-related sessions:

- `jobs/id/id_title`
- `api/jobs/id/other_jobs?offset=0&limit=5`
- `api/users/id/template`
- `@user_id`
- `api/recommend_specialty`

## Interpretation Guardrails

Very important:

- `event_count >= 20` does NOT mean "20 applicants"
- It means "this URL appeared at least 20 times inside apply-related sessions"
- This threshold is only used for readable URL-level interpretation
- It does NOT delete raw data or remove sessions from the main session-level analysis

Current threshold coverage:

- Threshold: `event_count >= 20`
- Kept URL rows: `13,489`
- Kept event share: `80.84%`
- Kept session share: `66.45%`

This threshold was chosen to reduce long-tail noise while keeping most meaningful traffic.

## Re-run Order

If you want to rebuild everything from scratch tomorrow, run in this order.

### 1. Rebuild the MySQL stage table

```powershell
cd C:\Users\hetmi\Downloads\playground\midproject1
$env:MYSQL_PASSWORD='12345'
.\.venv\Scripts\python.exe scripts\run_sql_file.py sql\build_topic1_log_stage.sql --host 127.0.0.1 --port 3306 --user root --database midproject1
```

### 2. Export monthly parquet from MySQL

```powershell
cd C:\Users\hetmi\Downloads\playground\midproject1
$env:MYSQL_PASSWORD='12345'
.\.venv\Scripts\python.exe scripts\export_topic1_stage_monthly_parquet.py --host 127.0.0.1 --port 3306 --user root --database midproject1 --chunksize 200000
```

### 3. Build monthly session artifacts

```powershell
cd C:\Users\hetmi\Downloads\playground\midproject1
.\.venv\Scripts\python.exe scripts\build_topic1_monthly_session_artifacts.py --memory-limit 2GB --threads 2
```

### 4. Generate the PDF report

```powershell
cd C:\Users\hetmi\Downloads\playground\midproject1
$env:MYSQL_PASSWORD='12345'
.\.venv\Scripts\python.exe scripts\generate_ex1_pdf_report.py --host 127.0.0.1 --port 3306 --user root --database midproject1
```

## Best Notebook To Continue From

Use this notebook first:

- `notebooks/05_ex1_user_session_apply_analysis_monthly_pipeline.ipynb`

Why:

- It does not load the giant raw CSVs directly
- It uses DuckDB aggregation on parquet
- It only loads filtered URL summaries into pandas
- It is the safest notebook for this machine

## Next Best Analysis Step

The current analysis is still session-pattern focused.

The strongest next step is:

1. Join `Application.csv`
2. Separate "apply-related sessions" from "sessions that ended in a real application"
3. Compare converted vs non-converted sessions

After that, also join:

1. `JobBookmark.csv`
2. Check whether bookmarking behavior is associated with later conversion

## Known Limitations

- Sessions are computed month by month
- A small number of sessions near month boundaries may split into two sessions
- The PDF report currently uses English body text for font stability
- URL-level filtering is for readability, not for applicant counting

## If You Want To Continue Immediately Tomorrow

Open:

- `notebooks/05_ex1_user_session_apply_analysis_monthly_pipeline.ipynb`

Then do one of these:

1. Inspect route groups and top URLs for product insight writing
2. Join `Application.csv` to move from "apply-related" to "actual conversion"
3. Expand the report with stronger business recommendations
