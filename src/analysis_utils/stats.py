from math import sqrt

from scipy.stats import norm


def two_proportion_z_test(
    control_successes: int,
    control_trials: int,
    treatment_successes: int,
    treatment_trials: int,
) -> dict:
    control_rate = control_successes / control_trials
    treatment_rate = treatment_successes / treatment_trials

    pooled_rate = (control_successes + treatment_successes) / (
        control_trials + treatment_trials
    )
    standard_error = sqrt(
        pooled_rate
        * (1 - pooled_rate)
        * ((1 / control_trials) + (1 / treatment_trials))
    )

    z_score = (treatment_rate - control_rate) / standard_error
    p_value = 2 * (1 - norm.cdf(abs(z_score)))

    return {
        "control_rate": control_rate,
        "treatment_rate": treatment_rate,
        "absolute_lift": treatment_rate - control_rate,
        "relative_lift": (treatment_rate - control_rate) / control_rate
        if control_rate
        else None,
        "z_score": z_score,
        "p_value": p_value,
    }
