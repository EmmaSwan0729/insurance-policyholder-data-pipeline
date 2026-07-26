"""
Unit tests for src/transform/enrichment.py
"""

from datetime import date

from pyspark.sql.types import (
    BooleanType,
    DateType,
    StringType,
    StructField,
    StructType,
)

from src.transform.enrichment import (
    calculate_policy_duration,
    filter_dq_passed,
    flag_lapse,lapse_rate_by_segment,
)


SCHEMA = StructType(
    [
        StructField("policy_id", StringType(), True),
        StructField("policy_type", StringType(), True),
        StructField("policy_status", StringType(), True),
        StructField("policy_start_date", DateType(), True),
        StructField("dq_status", StringType(), True),
    ]
)


def _rows(spark, records):
    return  spark.createDataFrame(records, schema=SCHEMA)


def test_calculate_policy_duration(spark):
    df = _rows(
        spark,
        [("POL-1","Term Assurance", "Active", date(2020,1,1), "PASS")],
    )
    result = calculate_policy_duration(df, reference_date=date(2025,1,1)).collect()[0]
    # 2020-01-01 to 2025-01-01 is exactly 5 years (accounting for one leap day
    # inside 365.25-day years), so this should land very close to 5.0.
    assert abs(result["policy_duration_years"] - 5.0) < 0.05


def test_flag_lapse_true_for_lapsed_status(spark):
    df = _rows(
        spark,
        [("POL-1","Term Assurance", "Lapsed", date(2020,1,1), "PASS")],
    )
    result = flag_lapse(df).collect()[0]
    assert result["is_lapsed"] is True


def test_flag_lapse_false_for_active_status(spark):
    df = _rows(
        spark,
        [("POL-1","Term Assurance", "Active", date(2020,1,1), "PASS")],
    )
    result = flag_lapse(df).collect()[0]
    assert result["is_lapsed"] is False


def test_filter_dq_passed_excludes_blocked(spark):
    df = _rows(
        spark,
        [
            ("POL-1","Term Assurance", "Active", date(2020,1,1), "PASS"),
            ("POL-2","Term Assurance", "Active", date(2020,1,1), "DEGRADED"),
            ("POL-3","Term Assurance", "Active", date(2020,1,1), "BLOCKED"),
        ],
    )
    result = filter_dq_passed(df)
    remaining_ids = {row["policy_id"] for row in result.collect()}
    assert remaining_ids == {"POL-1","POL-2"}


def test_lapse_rate_by_segment_basic(spark):
    # Term Assurance: 1 of 2 eligible policies lapsed -> 0.5
    # Whole of Life: 0 of 1 eligible policies lapsed -> 0.0
    # The Matured Term Assurance row must be excluded from the denominator.
    df = _rows(
        spark,
        [
            ("POL-1","Term Assurance", "Lapsed", date(2020,1,1), "PASS"),
            ("POL-2","Term Assurance", "Active", date(2020,1,1), "PASS"),
            ("POL-3","Term Assurance", "Matured", date(2020,1,1), "PASS"),
            ("POL-4","Whole of Life", "Active", date(2015,1,1), "PASS"),
        ],
    )
    enriched = flag_lapse(df)
    result = {
        row["policy_type"]: row["lapse_rate"]
        for row in lapse_rate_by_segment(enriched, "policy_type").collect()
    }
    assert result["Term Assurance"] == 0.5
    assert result["Whole of Life"] == 0.0



