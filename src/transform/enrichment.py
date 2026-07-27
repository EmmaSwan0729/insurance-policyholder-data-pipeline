"""
Silver -> Gold -- Transform & Enrichment.

Derives assumption-setting-relevant metrics from records that passed the
DQ Gate (PASS or DEGRADED; BLOCKED records are excluded and must be
resolved upstream before they can be enriched).

Metrics produced here mirror what an actuarial assumption-setting exercise
would consume directly:
- policy_duration_years: how long a policy has been in force
- is_lapsed: a clean boolean lapse indicator, driven by policy_status
- lapse_rate_by_segment: lapse rate broken down by any categorical column
  (e.g. policy_type, distribution_channel), which is the core metric for
  setting lapse assumptions per segment.
"""

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Policies with these statuses have exited the book through a route other
# than lapsing (maturity, a claim) and are excluded from the lapse rate
# denominator -- including them would understate the true lapse rate among
# policies that were actually exposed to the risk of lapsing.
LAPSE_ELIGIBLE_STATUSES = ("Active", "Lapsed")


def filter_dq_passed(df: DataFrame) -> DataFrame:
    # Only keep PASS and DEGRADED records continue downstream.
    return df.filter(F.col("dq_status") != "BLOCKED")


def calculate_policy_duration(
    df: DataFrame, reference_date: date | None = None
) -> DataFrame:
    """
    Adds policy_duration_years, measured from policy_start_date to
    reference_date (defaults to the current date if not supplied).
    A fixed reference_date is used in tests so results are deterministic.
    """
    reference_date_col = (
        F.lit(reference_date) if reference_date is not None else F.current_date()
    )
    return df.withColumn(
        "policy_duration_years",
        F.round(
            F.date_diff(reference_date_col, F.col("policy_start_date")) / 365.25, 2
        ),
    )


def flag_lapse(df: DataFrame) -> DataFrame:
    """Adds a clean boolean is_lapsed indicator, driven by policy_status."""
    return df.withColumn("is_lapsed", F.col("policy_status") == "Lapsed")


def enrich(df: DataFrame, reference_date: date | None = None) -> DataFrame:
    """Runs the full Silver -> Gold enrichment sequence on DQ-Gated data."""
    df = filter_dq_passed(df)
    df = calculate_policy_duration(df, reference_date=reference_date)
    df = flag_lapse(df)
    return df


def lapse_rate_by_segment(df: DataFrame, segment_col: str) -> DataFrame:
    """
    Computes lapse rate per value of segment_col, restricted to policies
    that were actually eligible to lapse (see LAPSE_ELIGIBLE_STATUSES).
    Requires enrich() to have been run first (needs is_lapsed).
    """
    eligible = df.filter(F.col("policy_status").isin(*LAPSE_ELIGIBLE_STATUSES))
    return (
        eligible.groupBy(segment_col)
        .agg(
            F.sum(F.col("is_lapsed").cast("int")).alias("lapsed_count"),
            F.count("*").alias("eligible_count"),
        )
        .withColumn(
            "lapse_rate", F.round(F.col("lapsed_count") / F.col("eligible_count"), 4)
        )
        .orderBy(F.desc("lapse_rate"))
    )
