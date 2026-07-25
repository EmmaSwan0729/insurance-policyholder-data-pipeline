"""
Audit trail logging.

Records every transformation applied during the Silver->Gold step, to
support internal and external audit queries.

Schema (proposed):
- transformation_id
- source_record_id
- rule_applied
- old_value
- new_value
- timestamp

TODO:
- log_transformation(record_id, rule, old_value, new_value) -> None
- write_audit_log(entries, table_name) -> None
"""
