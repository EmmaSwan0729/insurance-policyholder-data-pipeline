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
        ├──▶ Audit trail (every correction logged)
        │
        └──▶ Gold layer (Unity Catalog tables)
                │
                ▼
        Streamlit dashboard (data quality + business metrics)
```

Deployed end to end on Databricks Free Edition: Bronze, Gold, and audit
tables are written as governed Unity Catalog tables under
`workspace.policyholder_pipeline`, and the pipeline runs as a Databricks
Job defined via a Declarative Automation Bundle (`databricks.yml`).

## Tech Stack

- **Databricks Free Edition** — serverless Spark, Unity Catalog governance
- **PySpark / Spark SQL**
- **Delta Lake** — Bronze/Silver/Gold medallion architecture
- **Unity Catalog** — governed tables and volumes (`workspace.policyholder_pipeline`)
- **Declarative Automation Bundles** (formerly Asset Bundles) — CI/CD-ready deployment
- **GitHub Actions** — automated test pipeline
- **pytest** — unit tests for DQ rules, transformation, and audit logic
- **Streamlit + Plotly** — data quality and business metrics dashboard

## Project Status

- [x] UK-adapted source data with intentionally injected data quality issues
- [x] Bronze layer ingestion (PySpark + Delta)
- [x] DQ Gate — PASS / DEGRADED / BLOCKED classification (9 unit tests)
- [x] Transform & Enrichment — policy duration, lapse flag, lapse rate by
      segment (5 unit tests)
- [x] Audit trail — every DQ Gate correction logged (4 unit tests)
- [x] Pipeline orchestration script, environment-aware (local vs Databricks)
- [x] Streamlit dashboard (data quality, business metrics, audit trail)
- [x] Deployed to Databricks Free Edition via Declarative Automation Bundle
- [ ] CI/CD: automatic deployment to Databricks on merge to main
- [ ] Lapse rate prediction model (planned extension, see docs/project-plan.md)

## Deployment

The pipeline is deployed to Databricks Free Edition as a Job, orchestrated
by `scripts/run_pipeline.py`. The same script runs unmodified in both
environments — it detects whether it is running on Databricks
(`DATABRICKS_RUNTIME_VERSION` environment variable) and switches between
local file paths and Unity Catalog table writes accordingly.

**Successful job run:**

![Databricks job run succeeded](docs/screenshots/databricks-job-run.png)

**Unity Catalog tables produced by the pipeline:**

![Unity Catalog tables](docs/screenshots/unity-catalog-tables.png)

To deploy and run it yourself (requires a Databricks Free Edition workspace
and the Databricks CLI):

```bash
databricks bundle deploy -t dev
databricks bundle run run_pipeline -t dev
```

## Repository Structure

```
├── data/
│   └── raw/            # source CSVs (small, CC0-licensed — committed directly)
├── src/
│   ├── ingestion/      # Bronze layer: source data loading
│   ├── dq_gate/        # Silver layer: data quality validation rules
│   ├── transform/       # Silver→Gold: enrichment & derived metrics
│   └── audit/           # Audit trail logging
├── scripts/
│   ├── adapt_uk_data.py    # Kaggle source → UK-adapted dataset with injected DQ issues
│   └── run_pipeline.py     # Orchestrates the full pipeline, local or Databricks
├── tests/               # pytest unit tests (18 tests across DQ Gate, Transform, Audit)
├── dashboard/            # Streamlit app
├── docs/
│   ├── project-plan.md      # design notes, phased roadmap
│   └── screenshots/          # deployment evidence referenced in this README
├── databricks.yml       # Declarative Automation Bundle configuration
└── .github/workflows/    # CI pipeline (test on every push)
```

## Data Source

Base dataset: [Life Insurance Retention Dataset](https://www.kaggle.com/datasets/ayushyajnik/life-insurance-retention-dataset)
(Kaggle, CC0 Public Domain, 10,000 synthetic records), adapted to a UK life
insurance context (GBP, UK product types, UK date format) with intentionally
injected data quality issues to exercise the DQ Gate. See
`scripts/adapt_uk_data.py` for the full adaptation logic.

## Running Locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python scripts/adapt_uk_data.py    # regenerate the UK-adapted dataset (optional — already committed)
python scripts/run_pipeline.py     # run the full pipeline locally
pytest tests/ -v                   # run the test suite
streamlit run dashboard/app.py     # launch the dashboard
```

## License

MIT — see [LICENSE](LICENSE)