# %%
from pathlib import Path

import pandas as pd

DATA_DIR = Path("data")

# %%
# Replace this with a real export path or a SQL query result once the database
# connection is wired up.
sample_path = DATA_DIR / "sample.csv"

if sample_path.exists():
    df = pd.read_csv(sample_path)
    display(df.head())
    display(df.describe(include="all"))
else:
    print(f"Add a CSV export at {sample_path} to start exploring data.")
