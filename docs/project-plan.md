# Project Plan

Target role: M&G — Data Engineer / Data Analyst
Locations: Kildean/Stirling, Edinburgh, Reading
Close date: 3rd August 2026

## Phase 1 (must-have, before close date)

- [x] Data acquisition & UK adaptation (Kaggle base dataset → UK fields, GBP,
      UK product types, injected dirty data)
- [x] Bronze layer ingestion
- [x] DQ Gate (Silver layer): PASS / DEGRADED / BLOCKED classification
- [x] Transform & Enrichment (Gold layer): policy duration, lapse flag,
      lapse rate by segment
- [x] Audit trail logging
- [x] Unit tests (pytest) for DQ rules + transformation logic
- [ ] CI/CD minimal loop: push → test → deploy to dev (GitHub Actions +
      Databricks Declarative Automation Bundles (Asset Bundles)) — test
      stage is live in GitHub Actions; deploy stage is still manual via CLI,
      not yet automated
- [x] Streamlit dashboard: data quality overview + business metrics

## Phase 2 (extension)

- [ ] Lapse rate prediction model (logistic regression / XGBoost) on top of
      the same Delta tables — demonstrates downstream ML readiness without
      overstepping into actuarial modelling itself

## Phase 3 (optional, separate exploration)

- [ ] Containerise the dashboard (Docker, optionally minikube) — kept as an
      appendix, not part of the core narrative

## Design principles

- Core responsibility mirrors the JD: pipeline validates/transforms/enriches
  data for actuarial modelling — it does not build actuarial models.
- Architecture is modular (ingestion / dq_gate / transform / audit are
  decoupled) so Phase 2/3 can be added without rework.
- Scalability is demonstrated through design choices (Liquid Clustering,
  parameterised data volume, incremental processing via CDF) rather than
  by literally running at large scale on Free Edition.

## Scalability — Liquid Clustering (Completed 27 July 2026)

Liquid Clustering is live on `gold_policyholders_enriched`, clustered on
`policy_type` and `distribution_channel` (the two most common filter
dimensions for lapse rate analysis).

### Implementation note

The PySpark `DataFrameWriter.clusterBy()` method (chained directly onto
`.saveAsTable()`) did not register the clustering configuration on this
Databricks Free Edition workspace — `SHOW TBLPROPERTIES` showed no
`clusteringColumns` property afterwards, despite matching the officially
documented syntax exactly. Switched to a two-step approach instead: a
plain `saveAsTable()` write, followed by an explicit
`ALTER TABLE ... CLUSTER BY (...)` SQL statement via `spark.sql()`. This
worked and was confirmed via `SHOW TBLPROPERTIES` (`clusteringColumns`
and `delta.feature.clustering = supported` both present).

```python
enriched_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)
spark.sql(f"ALTER TABLE {GOLD_TABLE} CLUSTER BY (policy_type, distribution_channel)")
```

### Interview talking points

- **What's genuinely true:** the core pipeline logic (`apply_dq_gate`,
  `enrich`, `build_dq_gate_audit_log`) is written entirely as declarative
  Spark DataFrame operations — no `.collect()`, no row-level Python loops
  — so the same code scales from 10K to millions of rows without logic
  changes. `lapse_rate_by_segment(df, segment_col)` takes any column as a
  parameter rather than hardcoding `policy_type`. The Gold table has both
  an ML-ready structure (label + feature columns) and a governed,
  clustered layout for query performance as data volume grows.
- **What's NOT true and shouldn't be overclaimed:** the pipeline has
  never run against more than 10,000 rows, so this is "designed for
  scalability," not "tested at scale." Liquid Clustering's benefit (file
  skipping on filtered queries) isn't empirically measurable at this data
  volume — the configuration is correct, but there was no
  before/after query-performance comparison.
- **The clusterBy debugging story** — a good example to have ready: the
  PySpark `.clusterBy()` API silently failed to register despite matching
  documented syntax exactly. Diagnosed via `SHOW TBLPROPERTIES` (the
  officially recommended verification method) rather than trusting that
  "no error was thrown" meant success, then switched to a more reliable
  SQL-level `ALTER TABLE ... CLUSTER BY` statement and re-verified.
  Illustrates verifying claims rather than assuming success.

## Next Session (27 July 2026)

- [ ] Lint tooling: add `ruff` to CI (`.github/workflows/ci.yml`), run
      `ruff check .` as a step before `pytest`. Not yet installed —
      `requirements.txt` has no lint tooling at all currently.
      
## Open questions

- [ ] Final field mapping from Kaggle source → UK schema
- [ ] Time budget: fast version (1–2 days) vs full version (3–5 days)
- [ ] Databricks Jobs/Workflows orchestration — hands-on or design-only?
- [ ] Final CV/project description wording
