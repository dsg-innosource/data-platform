# Data Platform

The one-stop-shop for data governance, analytics, and data pipeline work at InnoSource. This repo follows a **Data as a Product** methodology — every data asset should have clear purpose, defined users, ownership, and documentation.

## What's In Here

### Data Processes

Self-contained Python pipelines, each with a `process.py` entry point:

| Process | What It Does | Frequency |
|---------|-------------|-----------|
| [adp-headcount](adp-headcount/) | Loads ADP employee data into the warehouse (bronze → silver) | Weekly |
| [monthly-billing](monthly-billing/) | Transforms ClickUp time tracking into client billing reports | Monthly |
| [swim-campaigns](swim-campaigns/) | Targeted candidate searches for recruiter outreach | On-demand |
| [resume-fraud-detection](resume-fraud-detection/) | Detects fraudulent resumes via employment history matching | Weekly (in development) |

### Metabase Models

Version-controlled SQL for our foundational Metabase models. See [metabase/catalog.yaml](metabase/catalog.yaml) for the full registry mapping DP-IDs to Metabase card IDs.

### Silver Layer Transforms

SQL scripts for bronze → silver transformations in [transforms/](transforms/). These are executed by Dagster on a schedule.

### Orchestration

Pipeline scheduling, monitoring, and alerting via [Dagster Cloud](https://dagster.io). Dagster definitions live in [orchestration/](orchestration/). See [docs/orchestration/dagster-setup.md](docs/orchestration/dagster-setup.md) for configuration details.

## Architecture

```
Source Systems → Airbyte (EL) → PostgreSQL Bronze → Transforms (SQL) → PostgreSQL Silver → Metabase
                                                     ↑                                      ↑
                                                 Dagster Cloud                          Dashboards
                                                 (orchestrates)                         & Reports
```

**Stack:** PostgreSQL (DO) · Airbyte (ELT) · Dagster Cloud (orchestration) · Metabase (visualization) · GitHub (version control)

## Quick Start

```bash
# Clone and set up
git clone <repository-url>
cd data-platform
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run a process (example: ADP in test mode)
python adp-headcount/process.py --test-mode

# Run Dagster locally for development
cd orchestration
pip install -e .
dagster dev
```

## Documentation

| Topic | Location |
|-------|----------|
| How to run each process | [docs/guides/](docs/guides/) |
| Technical architecture | [docs/technical/](docs/technical/) |
| Database schemas | [docs/database/](docs/database/) |
| Dagster setup & operations | [docs/orchestration/](docs/orchestration/) |
| Repo reorganization plan | [REPO_RECOMMENDATIONS.md](REPO_RECOMMENDATIONS.md) |
| Data Governance docs (ClickUp) | [ClickUp Docs](https://app.clickup.com/8673329/v/dc/88p1h-7051) |

## Data Security

PII and sensitive data is excluded from git via `.gitignore`. Never commit Excel files from ADP, CSV exports from ClickUp, billing output files, fraud detection results, or database credentials. See [CLAUDE.md](CLAUDE.md) for the full list of protected paths.

## Contributing

1. Follow established patterns — self-contained process folders, `process.py` naming, `config.yaml` for settings
2. Keep SQL transforms in `transforms/` as standalone `.sql` files (not embedded in Python)
3. Update documentation when you change things
4. Test before pushing — Dagster Cloud auto-deploys from `main`

---

**Owner:** Sean Beal · **Team:** Data Governance / Analytics · **Last Updated:** February 2026
