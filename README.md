# Data Analysis Workspace

This workspace is set up for a VS Code-first workflow:

- SQL exploration against MySQL
- Notebook-style EDA in VS Code
- A/B test analysis in Python
- Reusable helper code under `src/`

## Suggested workflow

1. Connect to your MySQL database from VS Code using SQLTools or your preferred SQL extension.
2. Pull manageable result sets into `data/` or query directly from Python.
3. Do EDA and statistical testing from files in `notebooks/`.
4. Move reusable logic into `src/analysis_utils/`.

## First-time setup

Python is not installed yet on this machine, so the environment cannot be created until Python is added.

Once Python 3.11+ is available, run:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Project layout

- `notebooks/` notebook-style analysis files
- `sql/` saved SQL queries
- `src/analysis_utils/` reusable Python helpers

## Notes

- Keep raw exports out of git; `data/` and `results/` are ignored.
- For large tables, query aggregates or samples first instead of pulling entire tables into memory.
