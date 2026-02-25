# CLAUDE.md

Context for AI assistants working on this repository. For the human-readable project overview, see [README.md](README.md).

## What This Repo Is

This is InnoSource's data platform — the central repository for data governance, analytics, and data pipeline work. It's owned by a small data governance/analytics team (3 people, led by Sean Beal) that follows a **Data as a Product** methodology.

The repo contains five types of things:

1. **Data processes** — Python ETL pipelines in self-contained folders
2. **Metabase SQL** — Version-controlled SQL for foundational Metabase models
3. **Database objects** — Version-controlled views, and eventually stored procedures, organized by object type in `database/`
4. **Silver layer transforms** — Active materialization scripts (future: dbt models) in `transforms/`
5. **Dagster orchestration** — Pipeline definitions, schedules, and sensors

## Architecture Decisions (Current)

These decisions were made in February 2026. See [REPO_RECOMMENDATIONS.md](REPO_RECOMMENDATIONS.md) for full rationale.

| Decision | Choice | Notes |
|----------|--------|-------|
| Orchestration | Dagster Cloud (Solo Plan, $10/month) | Manages scheduling, monitoring, alerting. Auto-deploys from GitHub. |
| Database objects | SQL in `database/` | Views in `database/views/`, future procs in `database/procs/`. Version-controlled DB definitions. |
| Transformation layer | Plain SQL scripts | In `transforms/` (when needed). Will migrate to dbt when we have 10+ transforms. |
| Data warehouse | PostgreSQL on Digital Ocean | Bronze/silver/gold schema architecture. |
| ELT | Airbyte → PostgreSQL bronze | Airbyte handles extract-load. We handle transform. |
| Visualization | Metabase | Connected to PostgreSQL. Base model SQL is version-controlled here. |
| Project management | ClickUp | Data Governance docs live in ClickUp, not this repo. |

### Future migration path

Dagster was chosen partly because of its first-class `dagster-dbt` integration. When transform count exceeds ~10, the team plans to adopt dbt Core. SQL files in `transforms/` and `database/` are written as standalone scripts specifically so they can become dbt models with minimal changes. Dagster remains the orchestrator regardless.

## Code Patterns to Follow

### Process folders

Every data process follows this pattern:

```
process-name/
├── input/              # Raw data files (gitignored)
├── archive/            # Processed files (gitignored)
├── output/             # Generated outputs (gitignored if contains PII)
├── modules/            # Supporting code (if complex)
├── process.py          # Main entry point (ALWAYS this name)
└── config.yaml         # All settings (NEVER hardcode)
```

When creating a new process, follow this exactly. The `process.py` naming convention is important — it's how both humans and Dagster find the entry point.

### SQL files

There are three homes for SQL in this repo, each with a distinct purpose:

- **`database/`** — Static database object definitions (views, future stored procedures). Organized by object type and schema layer (e.g., `database/views/silver/`). These are version-controlled DDL, not active processes.
- **`transforms/bronze_to_silver/`** — Active materialization scripts that Dagster executes to create/update silver tables. These will eventually become dbt models. Do NOT embed transform SQL in Python unless the transform requires Python logic (pandas, API calls, etc.).
- **`metabase/models/`** — Source-of-truth definitions for Metabase base models. When modifying, update both the `.sql` file and the Metabase model.

**Key distinction:** Views and other static DB objects go in `database/`, not `transforms/`. A view is a stored query definition, not a transform process.

### Configuration

All configuration is in `config.yaml` files per process, or environment variables for secrets. Database credentials come from `.env` (local) or Dagster Cloud secrets (production). Never hardcode credentials.

## What NOT to Break

### PII protection

These paths contain personally identifiable information and are gitignored. Never remove these gitignore rules:

- `adp-headcount/input/` — Employee data (Excel)
- `adp-headcount/archive/` — Processed employee files
- `monthly-billing/input/` — Time tracking data with names
- `monthly-billing/output/` — Billing reports with rates
- `monthly-billing/archive/` — Historical billing data
- `swim-campaigns/campaigns/*/*.csv` — Candidate contact lists
- `swim-campaigns/campaigns/*/*.sql` — Generated queries with PII
- `resume-fraud-detection/output/` — Fraud results with applicant PII

### Database schema

The data warehouse uses a medallion architecture:

- **Bronze** (`bronze.*`): Raw data from Airbyte and ETL processes. Do not transform in place.
- **Silver** (`silver.*`): Cleaned, standardized data. Created by transforms in this repo.
- **Gold** (`gold.*`): Business-ready aggregates and models. Used by Metabase dashboards.

Key tables:
- `bronze.adp_tenure_history` — Raw ADP employee snapshots (written by adp-headcount)
- `silver.fact_active_headcount` — Aggregated headcount metrics (calculated during ADP load)
- `silver.department_mapping` — Master department reference (DP-000)

See `docs/database/schemas.md` for the full schema.

### Existing process behavior

The four existing processes (adp-headcount, monthly-billing, swim-campaigns, resume-fraud-detection) are in production use. Changes to their `process.py` or `config.yaml` files should be tested thoroughly. ADP has a `--test-mode` flag that uses DuckDB instead of production PostgreSQL.

## Data Product Registry

Data products that have been formally assessed get a DP-ID (e.g., DP-000, DP-001). The canonical registry is in ClickUp ([Data Product Registry](https://app.clickup.com/8673329/v/dc/88p1h-7051)), with a local mapping in `metabase/catalog.yaml`.

Not all models have DP-IDs yet. Only assign one after the model has been assessed against the Data as a Product framework (clear purpose, defined users, ownership, documentation, trust/reliability, measurable value, lifecycle management).

## Dagster Orchestration

Pipeline definitions are in `orchestration/`. The project deploys automatically to Dagster Cloud when code is pushed to `main`.

Key concepts:
- **Assets** are defined in `orchestration/orchestration/assets/` — each represents a data product that Dagster materializes
- **Schedules** are in `schedules.py` — cron-based triggers for asset materialization
- **Sensors** are in `sensors.py` — event-driven triggers (future: Airbyte sync completion, file drops)
- **Resources** are in `resources/` — shared connections (PostgreSQL)

When adding a new scheduled pipeline, create an asset function, add it to the appropriate group, and add/update a schedule.

## Key Files

| File | Purpose |
|------|---------|
| `REPO_RECOMMENDATIONS.md` | Full reorganization plan with rationale for all decisions |
| `metabase/catalog.yaml` | Maps DP-IDs to Metabase card IDs and SQL files |
| `shared/database.yaml` | Shared database connection configuration |
| `.env` | Local environment variables (gitignored) |
| `requirements.txt` | Python dependencies |

## ClickUp Integration

Project tracking is in ClickUp space "DSG", list "DATA GOVERNANCE". Key tasks:
- **DSG-3308**: Data Platform Repo Reorganization (this work)
- **DSG-3234**: Department Mapping Matrix process

Data Governance documentation lives in ClickUp Docs (doc ID: 88p1h-7051), NOT in this repo. Don't move governance docs into the repo — ClickUp is where the team collaborates on those.

---

**Last Updated:** February 2026
