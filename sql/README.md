# SQL Folder

Recommended order for the current project:

1. `00_schema_export.sql`
   Use this to inspect table structure, size, indexes, and keys.
2. `01_log_event_mapping.sql`
   Profile top URLs first, then build a materialized event table from `log_2022` and `log_2023`.
3. `02_log_sessionization.sql`
   Convert the materialized event table into a materialized session table.
4. `03_ex1_behavior_patterns.sql`
   Explore common event groups, transitions, and short sequences.
5. `04_ex1_conversion_template.sql`
   Compare conversion vs non-conversion sessions after confirming application column names.

The ex1 workflow is intentionally split into small steps so each query stays understandable.
