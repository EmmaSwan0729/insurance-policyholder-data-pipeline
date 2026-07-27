"""
Unit tests for src/audit/trail.py
"""

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.audit.trail import build_audit_log, build_dq_gate_audit_log
from src.dq_gate.rules import apply_dq_gate

# Minimal schema for testing build_audit_log directly: it only ever needs
# policy_id plus the original/corrected column pair being audited.
MINIMAL_SCHEMA = StructType(
    [
        StructField("policy_id", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("gender_standardised", StringType(), True),
    ]
)

CORRECTIONS = [
    ("gender", "gender_standardised", "standardised gender casing/abbrevation")
]


def test_audit_log_captures_a_real_correction(spark):
    df = spark.createDataFrame(
        [("POL-1", "f", "Female")],
        schema=MINIMAL_SCHEMA,
    )
    result = build_audit_log(df, CORRECTIONS).collect()
    assert len(result) == 1
    row = result[0]
    assert row["policy_id"] == "POL-1"
    assert row["field_name"] == "gender"
    assert row["old_value"] == "f"
    assert row["new_value"] == "Female"


def test_audit_log_excludes_unchanged_values(spark):
    df = spark.createDataFrame(
        [("POL-1", "Female", "Female")],
        schema=MINIMAL_SCHEMA,
    )
    result = build_audit_log(df, CORRECTIONS).collect()
    assert len(result) == 0


def test_audit_log_excludes_unrecognised_values_left_untouched(spark):
    # "_RARE_" has no recognised mapping, so gender_standardised stays
    # "_RARE_" too -- nothing was corrected, so no audit row is expected.
    # (This case is still visible via dq_reason on the record itself.)
    df = spark.createDataFrame(
        [("POL-1", "_RARE_", "_RARE_")],
        schema=MINIMAL_SCHEMA,
    )
    result = build_audit_log(df, CORRECTIONS).collect()
    assert len(result) == 0


# --- Integration-style test: wiring build_dq_gate_audit_log to real DQ Gate output ---

DQ_GAATE_SCHEMA = StructType(
    [
        StructField("policy_id", StringType(), True),
        StructField("sum_assured_gbp", IntegerType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("health_status", StringType(), True),
        StructField("policy_status", StringType(), True),
        StructField("months_in_arrears", IntegerType(), True),
    ]
)


def test_dq_gate_audit_log_end_to_end(spark):
    df = spark.createDataFrame(
        [
            # Recoverable gender -> should produce one audit row
            ("POL-1", 250000, 40, "f", "Good", "Active", 0),
            # Already clean -> should produce no audit row
            ("POL-2", 250000, 40, "Male", "Good", "Active", 0),
        ],
        schema=DQ_GAATE_SCHEMA,
    )
    gated = apply_dq_gate(df)
    audit_log = build_dq_gate_audit_log(gated).collect()

    assert len(audit_log) == 1
    assert audit_log[0]["policy_id"] == "POL-1"
    assert audit_log[0]["old_value"] == "f"
    assert audit_log[0]["new_value"] == "Female"
