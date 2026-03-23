# %%
import numpy as np
import pandas as pd

from analysis_utils.stats import two_proportion_z_test

# %%
# Expected columns:
# - group: control / treatment
# - converted: 0 / 1
df = pd.DataFrame(
    {
        "group": ["control", "control", "treatment", "treatment"],
        "converted": [0, 1, 0, 1],
    }
)

summary = (
    df.groupby("group", as_index=False)
    .agg(conversions=("converted", "sum"), users=("converted", "size"))
    .assign(rate=lambda x: x["conversions"] / x["users"])
)

display(summary)

control = summary.loc[summary["group"] == "control"].iloc[0]
treatment = summary.loc[summary["group"] == "treatment"].iloc[0]

result = two_proportion_z_test(
    control_successes=int(control["conversions"]),
    control_trials=int(control["users"]),
    treatment_successes=int(treatment["conversions"]),
    treatment_trials=int(treatment["users"]),
)

result
