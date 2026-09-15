# BorealWatch

> Canadian environmental data platform for wildfire intelligence.

## Overview

BorealWatch is a research-oriented data engineering project focused on building
reliable, analytics-ready environmental datasets across Canada. The platform is
designed around heterogeneous wildfire, weather, air-quality, drought,
satellite, and geographic data sources.

The core engineering problem is not simply wildfire prediction. It is the
construction of interoperable spatial-temporal data pipelines that ingest,
validate, transform, model, and serve environmental data for analytics and
machine-learning workloads.

## Objectives

- Build repeatable batch ingestion pipelines.
- Standardize heterogeneous environmental datasets.
- Implement Bronze, Silver, and Gold data layers.
- Apply automated data-quality validation.
- Support spatial-temporal transformations.
- Build analytics-ready dimensional models.
- Add near-real-time event processing.
- Produce ML-ready environmental features.
- Document data lineage and architecture.

## Planned Architecture

```text
Official Canadian / public datasets
                |
                v
        Python ingestion
                |
                v
          Raw / Landing
                |
                v
     +------------------------+
     | Bronze - raw Delta     |
     +------------------------+
                |
                v
     +------------------------+
     | Silver - cleaned       |
     | validated / standardized
     +------------------------+
                |
                v
     +------------------------+
     | Gold - dimensional     |
     | analytics / ML features|
     +------------------------+
          |             |
          v             v
   Databricks SQL     Tableau
          |
          v
  ML / environmental insights
```

## Technology Stack

### Data Engineering

Python, SQL, PySpark, Apache Spark, Pandas, NumPy, Delta Lake

### Cloud / Platform

Azure Data Factory, ADLS Gen2, Azure Databricks, Event Hubs

### Data Quality

Schema validation, data cleansing, reconciliation, completeness,
uniqueness, consistency, and anomaly checks

### Analytics

Databricks SQL, Tableau

### Engineering

Git, GitHub Actions, Docker, Terraform, pytest

## Repository Structure

```text
src/                 Production Python package
notebooks/           Local analysis and exploration
databricks/          Databricks-specific workloads
data/                Local data-layer directories
tests/               Unit and integration tests
sql/                 SQL modeling and analytics
schemas/             Data contracts and schemas
config/              Pipeline configuration
docs/                Architecture and technical documentation
dashboards/           BI assets
infrastructure/      Terraform and container infrastructure
.github/             CI/CD workflows
```

## Development Status

**Phase 1 — In progress:** wildfire source discovery and raw ingestion.

Later phases will add environmental sources, PySpark transformations, Delta Lake
layers, data-quality controls, streaming, dimensional modeling, dashboards,
and ML feature engineering.

## Data Policy

Full source datasets are intentionally excluded from Git. The repository keeps
only code, schemas, documentation, and small test fixtures.

## Reproducibility

Environment dependencies are pinned in `requirements.txt`. Project metadata,
Python package configuration, and test configuration live in `pyproject.toml`.

## License

MIT. See `LICENSE` for details.
