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

### Database Objects

Version-controlled SQL definitions for database views, and eventually stored procedures, in [database/](database/). Organized by object type and schema layer (e.g., `database/views/silver/`).

### Silver Layer Transforms

Active materialization scripts for bronze → silver transformations in [transforms/](transforms/). These are executed by Dagster on a schedule. Static database objects like views belong in `database/`, not here.

### Production Reports

Daily and weekly operational reports — recruiter activity, position status, terminations — that get emailed each morning. The reporting framework lives in [reporting/](reporting/) and runs as Dagster assets. Each report is a structured pipeline (deterministic SQL + rule-based callouts + LLM-refined narrative + HTML/PDF rendering + Power Automate email) rather than a Claude prompt. See [reporting/routines/_framework.md](reporting/routines/_framework.md) for the architecture and [reporting/routines/recruiter-activity.md](reporting/routines/recruiter-activity.md) for the first migrated report.

### Orchestration

Pipeline scheduling, monitoring, and alerting via [Dagster Cloud](https://dagster.io). Dagster definitions live in [orchestration/](orchestration/). See [docs/orchestration/dagster-setup.md](docs/orchestration/dagster-setup.md) for configuration details.

## Architecture

```
Source Systems → Airbyte (EL) → PostgreSQL Bronze → Transforms (SQL) → PostgreSQL Silver → Metabase Dashboards
                                                     ↑                          │
                                                 Dagster Cloud                  │
                                                 (orchestrates)                 ↓
                                                                        Production Reports
                                                                        (PDF + email)
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
2. Put database object definitions (views, procs) in `database/` — keep `transforms/` for active materialization scripts
3. Update documentation when you change things
4. Test before pushing — Dagster Cloud auto-deploys from `main`

---

**Owner:** Sean Beal · **Team:** Data Governance / Analytics · **Last Updated:** February 2026
