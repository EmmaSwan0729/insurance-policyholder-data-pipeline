"""
Bronze layer ingestion.

Loads the raw (UK-adapted) policyholder source data as-is, with no cleaning
or transformation, into a Delta table. This preserves an untouched snapshot
of the source data for audit/traceability purposes.

TODO:
- read_raw_policyholders(spark, source_path) -> DataFrame
- write_bronze(df, table_name)
"""
