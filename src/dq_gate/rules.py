"""
Silver layer -- DQ Gate.

Classifies every record as PASS / DEGRADED / BLOCKED and attaches a
dq_status and dq_reason column, following three principles:

- BLOCKED: the record is unusable for any downstream purpose (e.g. no
  identifier, an economically meaningless sum assured, an implausible
  age). These records must not reach the Gold layer without manual review.
- DEGRADED: the record has an issue that can be safely standardised (e.g.
  inconsistent casing on a categorical value) or that represents a logical
  contradiction worth flagging (e.g. an "Active" policy that is materially
  in arrears). These records are usable but are marked for visibility.
- PASS: no issues found.

A record can trigger more than one rule; dq_reason lists every rule that
fired, semicolon-separated, so a single record's full issue history is
visible without re-running the checks.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

BLOCKED = "BLOCKED"
DEGRADED = "DEGRADED"
PASS_ = "PASS"

VALID_HEALTH_STATUSES = ("Excellent", "Good", "Fair")


def _standardise_gender(df: DataFrame) -> DataFrame:
    """
    Maps recognised gender variants (casing, abbreviations) to a
    standard Male/Female value. Values with no recognised mapping
    (e.g. the "_RARE_" placeholder) are left untouched and are caught
    separately as an "unrecognised category" issue.
    """
    return df.withColumn(
        "gender_standardised",
        F.when(F.lower(F.col("gender")).isin("m", "male"), F.lit("Male"))
        .when(F.lower(F.col("gender")).isin("f", "female"), F.lit("Female"))
        .otherwise(F.col("gender")),
    )


def apply_dq_gate(df: DataFrame) -> DataFrame:
    df = _standardise_gender(df)

    blocked_missing_policy_id = F.col("policy_id").isNull()
    blocked_non_positive_sum_assured = F.col("sum_assured_gbp") <= 0
    blocked_invalid_age = (F.col("age") < 0) | (F.col("age") > 120)

    degraded_gender_recovered = (F.col("gender") != F.col("gender_standardised")) & (
        F.col("gender_standardised").isin("Male", "Female")
    )
    degraded_unrecognised_gender = ~F.col("gender_standardised").isin("Male", "Female")
    degraded_unrecognised_health_status = ~F.col("health_status").isin(
        *VALID_HEALTH_STATUSES
    )
    degraded_status_arrears_conflict = (F.col("policy_status") == "Active") & (
        F.col("months_in_arrears") >= 6
    )

    blocked_condition = (
        blocked_missing_policy_id
        | blocked_non_positive_sum_assured
        | blocked_invalid_age
    )
    degraded_condition = (
        degraded_gender_recovered
        | degraded_unrecognised_gender
        | degraded_unrecognised_health_status
        | degraded_status_arrears_conflict
    )

    reason_exprs = [
        F.when(blocked_missing_policy_id, F.lit("missing policy_id")),
        F.when(
            blocked_non_positive_sum_assured, F.lit("sum_assured_gbp is not positive")
        ),
        F.when(blocked_invalid_age, F.lit("age outside plausible human range")),
        F.when(degraded_gender_recovered, F.lit("gender value standardised")),
        F.when(degraded_unrecognised_gender, F.lit("unrecognised gender category")),
        F.when(
            degraded_unrecognised_health_status,
            F.lit("unrecognised health_status category"),
        ),
        F.when(
            degraded_status_arrears_conflict,
            F.lit("policy_status Active conflicts with months_in_arrears >= 6"),
        ),
    ]

    df = df.withColumn("_reasons", F.array(*reason_exprs))
    df = df.withColumn(
        "dq_reason", F.array_join(F.expr("filter(_reasons, x -> x is not null)"), ";")
    )
    df = df.drop("_reasons")

    df = df.withColumn(
        "dq_status",
        F.when(blocked_condition, F.lit(BLOCKED))
        .when(degraded_condition, F.lit(DEGRADED))
        .otherwise(F.lit(PASS_)),
    )

    return df
