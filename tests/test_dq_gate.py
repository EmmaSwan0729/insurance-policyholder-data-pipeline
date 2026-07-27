"""
Unit tests for src/dq_gate/rules.py

Each test builds a minimal, hand-crafted record set that isolates a single
rule, so a failing test always points at exactly one broken piece of logic.
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.dq_gate.rules import BLOCKED, DEGRADED, PASS_, apply_dq_gate

# Explicit schema so a single-row test DataFrame with a None value (e.g. a
# missing policy_id) doesn't fail Spark's type inference, which needs at
# least one non-null value per column to guess a type on its own.
SCHEMA = StructType(
    [
        StructField("policy_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("marital_status", StringType(), True),
        StructField("number_of_dependents", IntegerType(), True),
        StructField("annual_income_gbp", IntegerType(), True),
        StructField("health_status", StringType(), True),
        StructField("smoking_status", StringType(), True),
        StructField("policy_type", StringType(), True),
        StructField("sum_assured_gbp", IntegerType(), True),
        StructField("monthly_premium_gbp", DoubleType(), True),
        StructField("distribution_channel", StringType(), True),
        StructField("policy_start_date", StringType(), True),
        StructField("months_in_arrears", IntegerType(), True),
        StructField("policy_status", StringType(), True),
    ]
)

BASE_ROW = {
    "policy_id": "POL-00000001",
    "customer_id": "cust-1",
    "age": 40,
    "gender": "Female",
    "marital_status": "Married",
    "number_of_dependents": 1,
    "annual_income_gbp": 60000,
    "health_status": "Good",
    "smoking_status": "Non-smoker",
    "policy_type": "Term Assurance",
    "sum_assured_gbp": 250000,
    "monthly_premium_gbp": 45.0,
    "distribution_channel": "Direct",
    "policy_start_date": "2018-01-01",
    "months_in_arrears": 0,
    "policy_status": "Active",
}


def _row(spark, **overrides):
    data = {**BASE_ROW, **overrides}
    ordered_values = [data[field.name] for field in SCHEMA.fields]
    return spark.createDataFrame([ordered_values], schema=SCHEMA)


def test_clean_record_passes(spark):
    df = _row(spark)
    result = apply_dq_gate(df).collect()[0]
    assert result["dq_status"] == PASS_
    assert result["dq_reason"] == ""


def test_blocked_when_policy_id_missing(spark):
    df = _row(spark, policy_id=None)
    result = apply_dq_gate(df).collect()[0]
    assert result["dq_status"] == BLOCKED
    assert "missing policy_id" in result["dq_reason"]


def test_blocked_when_sum_assured_non_positive(spark):
    df = _row(spark, sum_assured_gbp=0)
    result = apply_dq_gate(df).collect()[0]
    assert result["dq_status"] == BLOCKED
    assert "sum_assured_gbp is not positive" in result["dq_reason"]


def test_blocked_when_age_out_of_range(spark):
    df = _row(spark, age=150)
    result = apply_dq_gate(df).collect()[0]
    assert result["dq_status"] == BLOCKED
    assert "age outside plausible human range" in result["dq_reason"]


def test_degraded_when_gender_recoverable(spark):
    df = _row(spark, gender="f")
    result = apply_dq_gate(df).collect()[0]
    assert result["dq_status"] == DEGRADED
    assert result["gender_standardised"] == "Female"
    assert "gender value standardised" in result["dq_reason"]


def test_degraded_when_gender_unrecognised(spark):
    df = _row(spark, gender="_RARE_")
    result = apply_dq_gate(df).collect()[0]
    assert result["dq_status"] == DEGRADED
    assert "unrecognised gender category" in result["dq_reason"]


def test_degraded_when_health_status_unrecognised(spark):
    df = _row(spark, health_status="_RARE_")
    result = apply_dq_gate(df).collect()[0]
    assert result["dq_status"] == DEGRADED
    assert "unrecognised health_status category" in result["dq_reason"]


def test_degraded_when_status_conflicts_with_arrears(spark):
    df = _row(spark, policy_status="Active", months_in_arrears=8)
    result = apply_dq_gate(df).collect()[0]
    assert result["dq_status"] == DEGRADED
    assert (
        "policy_status Active conflicts with months_in_arrears >= 6"
        in result["dq_reason"]
    )


def test_blocked_takes_precedence_over_degraded(spark):
    # A record with both a BLOCKED-level and a DEGRADED-level issue
    # should be classified as BLOCKED, since that is the more severe outcome.
    df = _row(spark, policy_id=None, gender="f")
    result = apply_dq_gate(df).collect()[0]
    assert result["dq_status"] == BLOCKED
