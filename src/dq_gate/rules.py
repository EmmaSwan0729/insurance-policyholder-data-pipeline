"""
Silver layer — DQ Gate.

Classifies each record as PASS / DEGRADED / BLOCKED based on validation
rules, and writes `dq_status` + `dq_reason` columns alongside the data.

TODO:
- BLOCKED rules: e.g. missing policy_id, sum_assured <= 0
- DEGRADED rules: e.g. recoverable missing fields, inconsistent categorical
  values that can be standardised
- PASS: all checks succeeded
- apply_dq_gate(df) -> DataFrame (with dq_status, dq_reason columns)
"""
