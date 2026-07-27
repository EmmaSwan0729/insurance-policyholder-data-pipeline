"""
Orchestrates the full Bronze -> Silver (DQ Gate) -> Gold (Enrichment) ->
Audit pipeline end to end, and produces the extracts the Streamlit
dashboard reads.

The dashboard intentionally does not run Spark itself: it reads small,
pre-aggregated CSV extracts written here, the same way a BI tool would
query a curated serving layer rather than operating its own compute
cluster. This keeps the dashboard lightweight and matches how Databricks
dashboards typically consume Gold-layer tables rather than recomputing
them.
 
Run:
    python scripts/run_pipeline.py
"""
import os
import sys
from datetime import date

ON_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

if ON_DATABRICKS:
    # __file__ is not available inside Databricks' spark_python_task
    # execution context (the script runs via exec(), not a normal
    # interpreter invocation). The synced workspace files root is passed
    # in explicitly instead, via the job parameter set in databricks.yml.
    project_root = sys.argv[1]
else:
    from pathlib import Path
    project_root = str(Path(__file__).resolve().parent.parent)

sys.path.insert(0, project_root)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.audit.trail import build_dq_gate_audit_log, write_audit_log
from src.dq_gate.rules import apply_dq_gate
from src.ingestion.load_raw_data import get_spark_session, load_raw_policyholders, write_bronze
from src.transform.enrichment import enrich


if ON_DATABRICKS:
    SOURCE_PATH = "/Volumes/workspace/policyholder_pipeline/raw_data/uk_policyholders_source.csv"
else:
    SOURCE_PATH = "data/raw/uk_policyholders_source.csv"

BRONZE_PATH = "data/bronze/policyholders"  # local-only

CATALOG = "workspace"
SCHEMA = "policyholder_pipeline"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_policyholders"
GOLD_TABLE = f"{CATALOG}.{SCHEMA}.gold_policyholders_enriched"
AUDIT_TABLE = f"{CATALOG}.{SCHEMA}.audit_dq_corrections"

GOLD_PATH = "data/gold/policyholders_enriched"  # local-only
AUDIT_PATH = "data/audit/dq_corrections"

GOLD_CSV = "data/gold/policyholders_enriched.csv"
DQ_SUMMARY_CSV = "data/gold/dq_summary.csv"
DQ_REASON_SUMMARY_CSV = "data/gold/dq_reason_summary.csv"
AUDIT_CSV = "data/audit/dq_corrections.csv"
 
# Fixed reference date so policy_duration_years is reproducible across runs,
# rather than silently drifting as the calendar date changes.
REFERENCE_DATE = date(2025, 1, 1)


def build_dq_summary(gated_df: DataFrame) -> DataFrame:
    """Record counts per dq_status (PASS/DEGRADED/BLOCKED), including BLOCKED
    records that are excluded from the Gold layer -- the dashboard's data
    quality view needs the full picture, not just what survives to Gold."""
    return gated_df.groupBy("dq_status").count().orderBy("dq_status")


def build_dq_reason_summary(gated_df: DataFrame) -> DataFrame:
    """Frequency of each individual DQ reason across non-PASS records.
    A record with multiple reasons contributes to each reason's count."""
    return (
        gated_df.filter(F.col("dq_reason") != "")
        .withColumn("reason", F.explode(F.split(F.col("dq_reason"), "; ")))
        .groupBy("reason")
        .count()
        .orderBy(F.desc("count"))
    )


def main():
    spark = SparkSession.builder.getOrCreate() if ON_DATABRICKS else get_spark_session()

    raw_df = load_raw_policyholders(spark, SOURCE_PATH)
    if ON_DATABRICKS:
        raw_df.write.format("delta").mode("overwrite").saveAsTable(BRONZE_TABLE)
    else:
        write_bronze(raw_df, BRONZE_PATH)

    gated_df = apply_dq_gate(raw_df)
    dq_summary = build_dq_summary(gated_df)
    dq_reason_summary = build_dq_reason_summary(gated_df)

    enriched_df = enrich(gated_df, reference_date=REFERENCE_DATE)
    audit_log = build_dq_gate_audit_log(gated_df)

    if ON_DATABRICKS:
        enriched_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)
        spark.sql(f"ALTER TABLE {GOLD_TABLE} CLUSTER BY (policy_type, distribution_channel)")
        audit_log.write.format("delta").mode("append").saveAsTable(AUDIT_TABLE)
    else:
        enriched_df.write.format("delta").mode("overwrite").save(GOLD_PATH)
        write_audit_log(audit_log, AUDIT_PATH)

    # Lightweight extracts for the dashboard (see module docstring for why
    # the dashboard reads these instead of running Spark itself).
    if not ON_DATABRICKS:
        enriched_df.toPandas().to_csv(GOLD_CSV, index=False)
        dq_summary.toPandas().to_csv(DQ_SUMMARY_CSV, index=False)
        dq_reason_summary.toPandas().to_csv(DQ_REASON_SUMMARY_CSV, index=False)
        audit_log.toPandas().to_csv(AUDIT_CSV, index=False)

    if ON_DATABRICKS:
        print(f"Gold layer written: {enriched_df.count()} rows -> {GOLD_TABLE}")
        print(f"Audit log written: {audit_log.count()} rows -> {AUDIT_TABLE}")
    else:
        print(f"Gold layer written: {enriched_df.count()} rows -> {GOLD_PATH}")
        print(f"Audit log written: {audit_log.count()} rows -> {AUDIT_PATH}")
        print("Dashboard extracts written:")
        print(f"  {GOLD_CSV}")
        print(f"  {DQ_SUMMARY_CSV}")
        print(f"  {DQ_REASON_SUMMARY_CSV}")
        print(f"  {AUDIT_CSV}")
 
 
if __name__ == "__main__":
    main()