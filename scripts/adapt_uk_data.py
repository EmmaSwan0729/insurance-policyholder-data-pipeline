"""
Adapts the raw Kaggle "Life Insurance Retention Dataset" into a UK life
insurance context, and injects a controlled set of data quality issues for
the downstream DQ Gate to exercise.

Source:  data/raw/life_insurance_retention_dataset_full.csv (Kaggle, CC0)
Output:  data/raw/uk_policyholders_source.csv

This script represents the "source system extract" — i.e. what a real
upstream system might hand off to the pipeline before any Bronze/Silver/Gold
processing happens. It is intentionally NOT the DQ Gate itself: dirty rows
are injected here on purpose so the DQ Gate (src/dq_gate/rules.py) has
something real to catch.

Run:
    python scripts/adapt_uk_data.py
"""

import random
import uuid
from datetime import date, timedelta

import numpy as np
import pandas as pd

RANDOM_SEED = 42
SOURCE_PATH = "data/raw/life_insurance_retention_dataset_full.csv"
OUTPUT_PATH = "data/raw/uk_policyholders_source.csv"

DIRTY_FRACTION = 0.04  # ~4% of rows get an injected data quality issue

POLICY_TYPE_MAP = {
    "Term Life": "Term Assurance",
    "Whole Life": "Whole of Life",
    "Universal Life": "Whole of Life (Flexible Premium)",
    "Variable Life": "Investment-Linked Life",
}

DISTRIBUTION_CHANNELS = ["Direct", "IFA", "Bancassurance", "Online"]
DISTRIBUTION_WEIGHTS = [0.35, 0.30, 0.20, 0.15]

# UK policy start dates spread across a realistic multi-year window instead
# of the source data's single year (2022 only). Extended back to 2000 so a
# portion of long-duration policies can plausibly reach "Matured" status.
POLICY_START_RANGE = (date(2000, 1, 1), date(2024, 6, 30))


def random_date(start: date, end: date) -> date:
    delta_days = (end - start).days
    return start + timedelta(days=random.randint(0, delta_days))


def clean_customer_id(raw_id: str) -> str:
    """Strip the 'mostly' generator-tool prefix, keep the rest as a UUID-like id."""
    return (
        raw_id.replace("mostly", "").strip("-") if isinstance(raw_id, str) else raw_id
    )


def derive_policy_status(
    policy_start: date, months_in_arrears: int, rng: random.Random
) -> str:
    """
    Rule-based (not purely random) policy status, driven by policy duration
    and payment arrears, so downstream lapse-rate metrics tell a coherent
    story rather than being noise.
    """
    duration_years = (date(2025, 1, 1) - policy_start).days / 365.25

    if months_in_arrears >= 6:
        return "Lapsed"
    if duration_years >= 20 and rng.random() < 0.3:
        return "Matured"
    if rng.random() < 0.03:
        return "Claimed"
    return "Active"


def build_uk_dataset(df: pd.DataFrame) -> pd.DataFrame:
    rng = random.Random(RANDOM_SEED)
    np.random.seed(RANDOM_SEED)

    out = pd.DataFrame()

    out["policy_id"] = ["POL-" + str(uuid.uuid4())[:8].upper() for _ in range(len(df))]
    out["customer_id"] = df["customer_id"].apply(clean_customer_id)

    out["age"] = df["age"]
    out["gender"] = df["gender"]
    out["marital_status"] = df["marital_status"]
    out["number_of_dependents"] = df["number_of_dependents"]
    out["annual_income_gbp"] = df["annual_income"]
    out["health_status"] = df["health_status"]
    out["smoking_status"] = df["smoking_status"]

    out["policy_type"] = (
        df["policy_type"].map(POLICY_TYPE_MAP).fillna(df["policy_type"])
    )
    out["sum_assured_gbp"] = df["coverage_amount"]
    out["monthly_premium_gbp"] = df["monthly_premium"]

    out["distribution_channel"] = rng.choices(
        DISTRIBUTION_CHANNELS, weights=DISTRIBUTION_WEIGHTS, k=len(df)
    )

    # Spread policy_start_date across a realistic multi-year window
    start_dates = [random_date(*POLICY_START_RANGE) for _ in range(len(df))]
    out["policy_start_date"] = [d.isoformat() for d in start_dates]

    # months_in_arrears: mostly 0 (up to date), a tail of increasing arrears
    out["months_in_arrears"] = np.random.choice(
        [0, 1, 2, 3, 6, 9, 12],
        size=len(df),
        p=[0.80, 0.08, 0.05, 0.03, 0.02, 0.01, 0.01],
    )

    out["policy_status"] = [
        derive_policy_status(d, arrears, rng)
        for d, arrears in zip(start_dates, out["months_in_arrears"])
    ]

    return out


def inject_dirty_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Injects a controlled set of data quality issues at ~DIRTY_FRACTION of
    rows, split across BLOCKED-worthy and DEGRADED-worthy patterns, so the
    DQ Gate has real cases to classify.
    """
    rng = random.Random(RANDOM_SEED + 1)
    n = len(df)
    n_dirty = int(n * DIRTY_FRACTION)
    dirty_idx = rng.sample(range(n), n_dirty)

    # Split the dirty rows across issue types
    chunks = np.array_split(dirty_idx, 5)

    # 1) BLOCKED: non-positive sum_assured
    df.loc[chunks[0], "sum_assured_gbp"] = df.loc[chunks[0], "sum_assured_gbp"].apply(
        lambda x: -abs(x) if rng.random() < 0.5 else 0
    )

    # 2) BLOCKED: missing policy_id
    df.loc[chunks[1], "policy_id"] = None

    # 3) BLOCKED: age out of plausible human range
    df.loc[chunks[2], "age"] = [rng.choice([-5, 0, 130, 150]) for _ in chunks[2]]

    # 4) DEGRADED: inconsistent gender casing/abbreviation (recoverable via standardisation)
    gender_variants = {"Female": ["female", "F", "f"], "Male": ["male", "M", "m"]}
    for i in chunks[3]:
        current = df.at[i, "gender"]
        if current in gender_variants:
            df.at[i, "gender"] = rng.choice(gender_variants[current])

    # 5) DEGRADED: logical contradiction — Active status but heavily in arrears
    df.loc[chunks[4], "months_in_arrears"] = 8
    df.loc[chunks[4], "policy_status"] = "Active"

    return df


def main():
    print(f"Reading source data from {SOURCE_PATH} ...")
    raw = pd.read_csv(SOURCE_PATH)
    print(f"  {len(raw)} rows loaded")

    print("Building UK-adapted dataset ...")
    uk_df = build_uk_dataset(raw)

    print(f"Injecting dirty data (~{DIRTY_FRACTION:.0%} of rows) ...")
    uk_df = inject_dirty_data(uk_df)

    uk_df.to_csv(OUTPUT_PATH, index=False)
    print(f"Wrote {len(uk_df)} rows to {OUTPUT_PATH}")

    print("\n--- policy_status distribution ---")
    print(uk_df["policy_status"].value_counts())

    print("\n--- distribution_channel distribution ---")
    print(uk_df["distribution_channel"].value_counts())


if __name__ == "__main__":
    main()
