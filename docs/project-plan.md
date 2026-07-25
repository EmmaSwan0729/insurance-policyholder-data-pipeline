# Project Plan

Target role: M&G — Data Engineer / Data Analyst
Locations: Kildean/Stirling, Edinburgh, Reading
Close date: 3rd August 2026

## Phase 1 (must-have, before close date)

- [ ] Data acquisition & UK adaptation (Kaggle base dataset → UK fields, GBP,
      UK product types, injected dirty data)
- [ ] Bronze layer ingestion
- [ ] DQ Gate (Silver layer): PASS / DEGRADED / BLOCKED classification
- [ ] Transform & Enrichment (Gold layer): policy duration, lapse flag,
      lapse rate by segment
- [ ] Audit trail logging
- [ ] Unit tests (pytest) for DQ rules + transformation logic
- [ ] CI/CD minimal loop: push → test → deploy to dev (GitHub Actions +
      Databricks Asset Bundles)
- [ ] Streamlit dashboard: data quality overview + business metrics

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
  by literally running at large scale on Community Edition.

## Open questions

- [ ] Final field mapping from Kaggle source → UK schema
- [ ] Time budget: fast version (1–2 days) vs full version (3–5 days)
- [ ] Databricks Jobs/Workflows orchestration — hands-on or design-only?
- [ ] Final CV/project description wording
