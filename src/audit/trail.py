"""
Audit trail logging.
 
Produces a standalone log of every value correction the DQ Gate applied,
so audit queries can be answered by reading this table directly instead of
having to diff Bronze and Silver snapshots by hand. This module only
records *corrections* (a field value being changed) -- BLOCKED and
DEGRADED classifications that do not change a value (e.g. an unrecognised
health_status category, or a policy_status/arrears conflict) are already
visible via the dq_status and dq_reason columns on the record itself and
do not need a separate audit row.
 
Schema produced by build_audit_log:
- transformation_id: unique id for this audit entry
- policy_id: the record the correction was applied to
- field_name: which field was corrected
- old_value: the original, as-received value
- new_value: the corrected value
- rule_applied: human-readable description of why the correction was made
- transformed_at: when the correction was logged
"""

from typing import List, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

#
#
GENDER_CORRECTION = (
    "gender",
    "gender_standardised",
    "Standardised gender casing/abbreviation to Male/Female",
)

DQ_GATE_CORRECTIONS: List[Tuple[str, str,  str]] = [GENDER_CORRECTION]


def build_audit_log(df: DataFrame, corrections: List[Tuple[str, str, str]]) -> DataFrame:
    """
    Builds one audit row per record per correction where the original and
    corrected columns actually differ. A record untouched by a given
    correction contributes no row for it.
    """
    entries = []
    for original_col, corrected_col, rule_description in corrections:
        entry = (
            df.filter(F.col(original_col) != F.col(corrected_col))
            .select(
                F.expr("uuid()").alias("transformation_id"),
                F.col("policy_id"),
                F.lit(original_col).alias("field_name"),
                F.col(original_col).alias("old_value"),
                F.col(corrected_col).alias("new_value"),
                F.lit(rule_description).alias("rule_applied"),
                F.current_timestamp().alias("transformed_at"),
            )
        )
        entries.append(entry)

    audit_log = entries[0]
    for entry in entries[1:]:
        audit_log = audit_log.unionByName(entry)
    return audit_log


def build_dq_gate_audit_log(df: DataFrame) -> DataFrame:
    """Convenience wrapper for the corrections currently applied by the DQ Gate."""
    return build_audit_log(df, DQ_GATE_CORRECTIONS)


def write_audit_log(df: DataFrame, audit_path: str) -> None:
    """Appends audit entries to the audit Delta table. Audit history is never overwritten."""
    df.write.format("delta").mode("append").save(audit_path)


