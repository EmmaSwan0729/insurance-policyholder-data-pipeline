# Insurance Policyholder Data Validation & Assumption-Setting Pipeline

> A Databricks-based data pipeline that validates, transforms, and enriches
> UK life insurance policyholder data for use in actuarial modelling and
> assumption setting (lapse rate, retention, and related metrics).

## Motivation

This project is built to mirror a real-world data engineering role in the
UK financial services sector: operating and maintaining BAU (business-as-usual)
data pipelines that feed validated, trustworthy data into actuarial models —
without building the actuarial models themselves.

## Architecture

```
Raw source data (Kaggle, adapted to UK context)
        │
        ▼
   Bronze layer (raw ingestion, untouched)
        │
        ▼
   Silver layer — DQ Gate (PASS / DEGRADED / BLOCKED)
        │
        ▼
   Silver → Gold — Transform & Enrichment
   (policy duration, lapse flag, lapse rate by segment)
        │
        ├──▶ Audit trail (every transformation logged)
        │
        └──▶ Gold layer (curated tables for reporting / downstream use)
                │
                ▼
        Streamlit dashboard (data quality + business metrics)
```

## Tech Stack

- **Databricks** (Community Edition) — Spark, Spark SQL, PySpark
- **Delta Lake** — Medallion architecture, schema evolution, Liquid Clustering
- **Databricks Asset Bundles** — CI/CD deployment (dev/prod)
- **GitHub Actions** — automated test + deploy pipeline
- **pytest** — unit tests for DQ rules and transformation logic
- **Streamlit** — data quality & business metrics dashboard

## Project Status

🚧 In progress — see [docs/project-plan.md](docs/project-plan.md) for the
current build phase and roadmap.

## Repository Structure

```
├── data/               # raw / processed data (not committed — see .gitignore)
├── src/
│   ├── ingestion/      # Bronze layer: source data loading
│   ├── dq_gate/        # Silver layer: data quality validation rules
│   ├── transform/      # Silver→Gold: enrichment & derived metrics
│   └── audit/          # Audit trail logging
├── tests/              # pytest unit tests
├── dashboard/          # Streamlit app
├── notebooks/          # exploratory Databricks/Jupyter notebooks
├── docs/               # design notes, data dictionary, project plan
├── databricks.yml      # Databricks Asset Bundle config
└── .github/workflows/  # CI/CD pipeline
```

## Data Source

Base dataset: [Life Insurance Retention Dataset](https://www.kaggle.com/datasets/ayushyajnik/life-insurance-retention-dataset)
(Kaggle, CC0 Public Domain, 10,000 synthetic records), adapted to a UK life
insurance context (GBP, UK product types, UK date format) with intentionally
injected data quality issues to exercise the DQ Gate.

## License

MIT — see [LICENSE](LICENSE)
