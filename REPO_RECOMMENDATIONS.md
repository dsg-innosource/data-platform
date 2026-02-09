# Data Platform Repo — Reorganization Recommendations

**Author:** Sean Beal + Claude
**Date:** February 9, 2026
**Status:** Draft for Review

---

## Executive Summary

This document recommends a **practical, incremental reorganization** of the `data-platform` repo to serve as the one-stop-shop for your data governance/analytics work. The approach prioritizes three things:

1. **Get Metabase SQL into version control** (immediate value)
2. **Set up Dagster for orchestration** — scheduling, monitoring, and running SQL + Python pipelines
3. **Create a clear home for silver layer work** as it develops

The previous reorganization plan (DSG-3308) was over-engineered for where you are today. This proposal keeps what's already working, adds what's missing, and avoids unnecessary complexity.

### Key Decisions Made

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Orchestrator | **Dagster Cloud (Solo Plan)** | Purpose-built for data pipelines; handles SQL + Python; web UI for monitoring; first-class dbt integration when ready; managed service eliminates self-hosting complexity |
| Transformation layer | **SQL scripts (for now)** | Start simple; migrate to dbt when transform count justifies it |
| Metabase SQL scope | **Base models only** | 6 foundational models from "00 BASE MODELS" collection |
| Infrastructure | **Dagster Cloud + shut down n8n** | $10/month managed service; zero infrastructure to maintain; eliminates n8n droplet cost |

---

## Current State (What's Working Well)

Your repo is honestly in good shape. Four self-contained processes, each with a consistent pattern:

| Process | Frequency | Entry Point | Status |
|---------|-----------|-------------|--------|
| ADP Headcount | Weekly | `adp-headcount/process.py` | Mature, tested |
| Monthly Billing | Monthly | `monthly-billing/process.py` | Mature |
| Swim Campaigns | On-demand | `swim-campaigns/process.py` | Mature |
| Resume Fraud Detection | Weekly | `resume-fraud-detection/process.py` | In development |

**What to keep:** The folder-per-process pattern, the `process.py` convention, the shared database config, the docs structure, and the `.gitignore` PII protections. None of that needs to change.

---

## Proposed New Structure

```
data-platform/
├── README.md                          # Updated with new sections
├── CLAUDE.md                          # Keep as-is
├── requirements.txt                   # Keep as-is
├── .env                               # Keep as-is
├── .gitignore                         # Minor additions
│
├── .github/
│   └── workflows/
│       └── sql-validation.yml         # CI: validate SQL syntax on PRs
│
├── shared/
│   ├── database.yaml                  # Keep as-is
│   └── README.md                      # Keep as-is
│
│   ══════════════════════════════════
│   EXISTING PROCESSES (no changes)
│   ══════════════════════════════════
│
├── adp-headcount/                     # Keep as-is
├── monthly-billing/                   # Keep as-is
├── swim-campaigns/                    # Keep as-is
├── resume-fraud-detection/            # Keep as-is
│
│   ══════════════════════════════════
│   NEW: METABASE SQL (version control)
│   ══════════════════════════════════
│
├── metabase/
│   ├── README.md                      # How this folder works
│   ├── models/                        # Metabase "models" (base layer)
│   │   ├── dp000_department_mapping.sql
│   │   ├── dp001_requisition_application_progress.sql
│   │   ├── dp005_adp_tenure.sql
│   │   ├── ai_tenure_detail.sql
│   │   ├── applicant_sources.sql
│   │   └── requisitions.sql
│   └── catalog.yaml                   # Maps DP-IDs to Metabase card IDs
│
│   ══════════════════════════════════
│   NEW: SILVER LAYER TRANSFORMS
│   ══════════════════════════════════
│
├── transforms/
│   ├── README.md                      # How transforms work
│   └── bronze_to_silver/              # SQL scripts for bronze→silver
│       ├── fact_active_headcount.sql
│       └── (future transforms here)
│
│   ══════════════════════════════════
│   NEW: DAGSTER ORCHESTRATION
│   ══════════════════════════════════
│
├── orchestration/
│   ├── README.md                      # Setup and deployment guide
│   ├── pyproject.toml                 # Dagster project config
│   ├── setup.py                       # Package setup
│   └── orchestration/                 # Dagster definitions
│       ├── __init__.py                # Definitions entry point
│       ├── assets/
│       │   ├── __init__.py
│       │   ├── transforms.py          # Silver layer SQL as Dagster assets
│       │   └── fraud_detection.py     # Fraud detection as Dagster asset
│       ├── resources/
│       │   ├── __init__.py
│       │   └── postgres.py            # Shared DB connection
│       ├── schedules.py               # Cron schedules
│       └── sensors.py                 # Future: file sensors for ADP, etc.
│
│   ══════════════════════════════════
│   EXISTING DOCS (minor updates)
│   ══════════════════════════════════
│
├── docs/
│   ├── README.md
│   ├── guides/                        # Keep as-is
│   ├── technical/                     # Keep as-is
│   ├── database/                      # Keep as-is
│   └── orchestration/                 # NEW
│       └── dagster-setup.md           # Dagster Cloud config and operations guide
│
└── tests/                             # Keep as-is
```

### What Changed

Only **four things** are new:

1. **`metabase/`** — Your Metabase model SQL in version control
2. **`transforms/`** — A home for silver layer SQL scripts
3. **`orchestration/`** — Dagster project for scheduling and monitoring everything
4. **`docs/orchestration/`** — How to deploy and operate Dagster

Everything else stays exactly where it is.

---

## Part 1: Metabase SQL in Version Control

### What Goes In

Your 6 base models from the "00 BASE MODELS" collection:

| DP-ID | Card ID | Name | Type | Views |
|-------|---------|------|------|-------|
| DP-000 | 704 | Department Mapping | UI Builder | 1 |
| DP-001 | 305 | Requisition Application Progress | Native SQL | 709 |
| DP-005 | 331 | ADP Tenure | Native SQL | 108 |
| — | 288 | AI Tenure Detail | Native SQL | 235 |
| — | 506 | Applicant Sources | Native SQL | 24 |
| — | 545 | Requisitions | Native SQL | 32 |

### File Naming Convention

```
{dp_id}_{descriptive_name}.sql
```

If a model doesn't have a DP-ID yet, use the descriptive name only. When it gets assigned a DP-ID, rename the file.

### The Catalog File

`metabase/catalog.yaml` maps between your DP registry, Metabase card IDs, and file names:

```yaml
# metabase/catalog.yaml
# Maps Data Product IDs to Metabase cards and SQL files
# Keep this in sync when models change

models:
  - dp_id: DP-000
    name: Department Mapping
    metabase_card_id: 704
    file: models/dp000_department_mapping.sql
    query_type: ui_builder  # Note: this one uses Metabase UI, not raw SQL
    description: >
      Master reference linking ADP department codes to business attributes
      (service type, client, location). Used across all reports.
    depends_on: []

  - dp_id: DP-001
    name: Requisition Application Progress
    metabase_card_id: 305
    file: models/dp001_requisition_application_progress.sql
    query_type: native_sql
    description: >
      Applications with related requisitions and their progression through
      the hiring process.
    depends_on: []

  - dp_id: DP-005
    name: ADP Tenure
    metabase_card_id: 331
    file: models/dp005_adp_tenure.sql
    query_type: native_sql
    description: >
      Latest extract from ADP, used to calculate net gain, termination
      reasons, etc.
    depends_on: []

  - dp_id: null  # Assign DP-ID when ready
    name: AI Tenure Detail
    metabase_card_id: 288
    file: models/ai_tenure_detail.sql
    query_type: native_sql
    description: >
      Combines ADP employment data with AI assessment scores using
      multi-pass matching (initial hires, pipeline hires, rehires).
    depends_on:
      - DP-000  # Department Mapping for client resolution

  - dp_id: null
    name: Applicant Sources
    metabase_card_id: 506
    file: models/applicant_sources.sql
    query_type: native_sql
    description: >
      Source of applications (Indeed, LinkedIn, Facebook, etc.)
    depends_on: []

  - dp_id: null
    name: Requisitions
    metabase_card_id: 545
    file: models/requisitions.sql
    query_type: native_sql
    description: >
      Base requisition model.
    depends_on: []
```

### Workflow for Changes

The process for updating Metabase SQL going forward:

1. **Edit the `.sql` file** in the repo
2. **Commit and push** with a meaningful message
3. **Copy the SQL** into Metabase and save the model
4. **Update `catalog.yaml`** if metadata changed

This is manual sync for now, which is fine at your scale. If it becomes painful later, you can add a GitHub Action that validates SQL syntax on PR, or even a script that pushes SQL to Metabase via API.

### DP-000 Note (UI Builder)

Department Mapping (card 704) uses Metabase's UI query builder rather than raw SQL. For this one, store a SQL equivalent that produces the same result, with a comment at the top noting the difference:

```sql
-- DP-000: Department Mapping
-- NOTE: In Metabase this is a UI-builder model (card 704).
-- This SQL equivalent is maintained for version control and
-- portability. If you modify this, update the Metabase model too.

SELECT
    dept_number,
    dept_name,
    service,
    offering,
    client_name,
    location,
    is_active
FROM silver.department_mapping
ORDER BY dept_number;
```

---

## Part 2: Silver Layer Transforms

### The Simple Approach

You already have one silver layer transform embedded in the ADP pipeline — `fact_active_headcount` is calculated as part of `adp-headcount/modules/load.py`. The idea here is to extract transforms like this into standalone SQL files that can be:

- Version controlled
- Run independently or via Dagster
- Eventually migrated to dbt if you choose to go that route

### Structure

```
transforms/
├── README.md
└── bronze_to_silver/
    ├── fact_active_headcount.sql
    └── (future transforms here)
```

Note there's no Python runner script here — Dagster handles execution (see Part 3). The `transforms/` directory is just clean SQL files, which makes them portable. If you switch to dbt later, these SQL files become dbt models with minimal modification.

### When to Extract vs. Leave Embedded

| Scenario | Recommendation |
|----------|---------------|
| Transform is part of a Python ETL pipeline (like ADP) | Leave it embedded for now |
| Transform is pure SQL with no Python logic | Put in `transforms/` |
| New silver layer work (upcoming) | Start in `transforms/` |
| Metabase model SQL that creates silver tables | Put in `transforms/` |

---

## Part 3: Orchestration with Dagster Cloud

### Why Dagster

Dagster is a data orchestrator built specifically for pipelines like yours. Compared to general-purpose automation tools (n8n, Zapier) or CI/CD tools (GitHub Actions), it understands data natively — assets, dependencies, schedules, data quality, and observability are all first-class concepts.

Key reasons for choosing it:

- **Runs both SQL and Python** — no hacks needed for either
- **Web UI** — visual DAG of your pipeline, run history, logs, schedules
- **First-class dbt integration** — when you're ready, `dagster-dbt` treats dbt models as native Dagster assets (see Appendix A)
- **Sensors** — can watch for webhooks, database changes, and more
- **Built-in alerting** — email/Slack notifications on failure, no configuration needed

### Why Dagster Cloud (Solo Plan) Instead of Self-Hosting

Self-hosting Dagster is significantly more complex than the "just pip install and run" pitch suggests. You'd need to configure the webserver, daemon, run storage, systemd services, nginx/HTTPS, and handle upgrades — all ongoing maintenance that takes time away from actual data work. Multiple practitioners have flagged documentation gaps and operational gotchas when self-hosting at production scale.

The Solo Plan ($10/month) eliminates all of that:

- **Zero infrastructure** — Dagster hosts the webserver, daemon, scheduler, and run storage
- **GitHub integration** — Connect your repo, push code, Dagster deploys it automatically
- **Built-in secrets management** — Store your DB credentials securely in Dagster Cloud
- **Managed compute** — Your Python and SQL run on Dagster's serverless infrastructure
- **Automatic upgrades** — Always on the latest version, no maintenance windows
- **30-day free trial** — Kick the tires before paying anything
- **5,000 credits/month included** — At your scale (~200-300 materializations/month), this is more than enough

**Net cost impact:** $10/month for Dagster Cloud minus whatever the n8n droplet costs. You may come out even or ahead.

### Architecture

```
┌──────────────────────────────────────────────────────────┐
│  Dagster Cloud (Solo Plan)                                │
│                                                           │
│  ┌─────────────────────────────────┐                      │
│  │  Dagster Cloud UI               │ ← Web UI (hosted)    │
│  │                                  │                      │
│  │  Schedules:                      │                      │
│  │    • Silver transforms (daily)   │                      │
│  │    • Fraud detection (weekly)    │                      │
│  │                                  │                      │
│  │  Sensors (future):               │                      │
│  │    • Airbyte sync complete       │                      │
│  │                                  │                      │
│  │  Serverless compute:             │                      │
│  │    • Runs your Python + SQL      │                      │
│  └──────────┬──────────────────────┘                      │
│             │                                             │
│             │  Connects to:                               │
│             ├──▶ DO PostgreSQL (your data warehouse)      │
│             └──▶ GitHub (auto-deploys on push)            │
└──────────────────────────────────────────────────────────┘

You manage: Code in GitHub + Postgres credentials in Dagster Cloud
Dagster manages: Everything else
```

### What Gets Orchestrated

| Asset / Job | Type | Schedule | What It Does |
|-------------|------|----------|-------------|
| Silver layer transforms | SQL | Daily (2am ET) | Runs `transforms/bronze_to_silver/*.sql` in order against Postgres |
| Fraud detection | Python | Weekly (Mon 6am ET) | Runs fraud detection logic, writes results to Postgres |
| (Future) ADP headcount | Python | Sensor: file drop | Processes ADP Excel file when detected |
| (Future) Monthly billing | Python | Monthly (1st business day) | Generates billing report |

### What a Dagster Asset Looks Like

Dagster organizes work around **assets** — things that your pipelines produce. Each asset is a Python function that creates or updates a piece of data. Here's what your silver layer transforms would look like:

```python
# orchestration/orchestration/assets/transforms.py

from dagster import asset, AssetExecutionContext
from ..resources.postgres import PostgresResource
from pathlib import Path

TRANSFORMS_DIR = Path(__file__).parents[3] / "transforms" / "bronze_to_silver"

@asset(
    group_name="silver_layer",
    description="Clean department mapping from bronze to silver"
)
def department_mapping_silver(context: AssetExecutionContext, postgres: PostgresResource):
    sql = (TRANSFORMS_DIR / "department_mapping.sql").read_text()
    rows = postgres.execute(sql)
    context.log.info(f"Department mapping: {rows} rows affected")

@asset(
    group_name="silver_layer",
    deps=["department_mapping_silver"],  # Runs AFTER department mapping
    description="Active headcount fact table"
)
def fact_active_headcount(context: AssetExecutionContext, postgres: PostgresResource):
    sql = (TRANSFORMS_DIR / "fact_active_headcount.sql").read_text()
    rows = postgres.execute(sql)
    context.log.info(f"Active headcount: {rows} rows affected")
```

Note the `deps=` parameter — that's how you specify execution order. Dagster builds a dependency graph and runs things in the right sequence. If `department_mapping_silver` fails, `fact_active_headcount` won't run.

And here's fraud detection as an asset:

```python
# orchestration/orchestration/assets/fraud_detection.py

from dagster import asset, AssetExecutionContext
from ..resources.postgres import PostgresResource

@asset(
    group_name="fraud_detection",
    description="Weekly resume fraud detection analysis"
)
def fraud_detection_results(context: AssetExecutionContext, postgres: PostgresResource):
    # Option A: Run SQL stored procedures
    postgres.execute("CALL fraud.detect_resume_anomalies()")

    # Option B: Run Python logic directly
    # results = run_fraud_analysis(postgres.connection)
    # postgres.insert("fraud.detection_results", results)

    context.log.info("Fraud detection complete")
```

### Schedules

```python
# orchestration/orchestration/schedules.py

from dagster import ScheduleDefinition, AssetSelection

silver_layer_schedule = ScheduleDefinition(
    name="daily_silver_transforms",
    target=AssetSelection.groups("silver_layer"),
    cron_schedule="0 7 * * *",  # Daily at 7am UTC (2am ET)
)

fraud_detection_schedule = ScheduleDefinition(
    name="weekly_fraud_detection",
    target=AssetSelection.groups("fraud_detection"),
    cron_schedule="0 11 * * 1",  # Monday 11am UTC (6am ET)
)
```

### Monitoring

Dagster Cloud's web UI gives you everything you'd need out of the box:

- **Run history** — every execution with pass/fail, duration, and full logs
- **Asset graph** — visual DAG showing dependencies between your transforms
- **Schedules** — see what's scheduled, when it last ran, next run time
- **Alerts** — built-in email/Slack notifications on failure (no extra configuration)
- **Asset health** — see freshness, staleness, and materialization history at a glance

For extra visibility, you can also have Dagster write run metadata to a `pipeline_runs` table in Postgres and build a Metabase dashboard that monitors pipeline health. Your data platform monitors itself.

### Getting Started with Dagster Cloud

Setup is dramatically simpler than self-hosting:

1. **Sign up** at dagster.io for the Solo Plan (30-day free trial)
2. **Connect your GitHub repo** — Dagster Cloud pulls code from your `orchestration/` directory
3. **Add secrets** — Store your DO Postgres credentials in Dagster Cloud's secrets manager
4. **Push code** — Every push to `main` auto-deploys your pipeline definitions
5. **Enable schedules** — Toggle on your silver layer and fraud detection schedules in the UI

That's it. No servers, no systemd, no nginx, no daemon configuration. The `docs/orchestration/dagster-setup.md` file would document your specific configuration (secrets, schedules, etc.) for team reference.

### Local Development

You still develop and test locally before pushing:

```bash
# Install Dagster locally for development
pip install dagster dagster-webserver dagster-postgres

# Run local dev server (uses your local .env for DB credentials)
cd data-platform/orchestration
dagster dev

# Opens web UI at http://localhost:3000
# Test your assets, preview the DAG, run transforms manually
```

When it looks good locally, push to GitHub and Dagster Cloud picks it up automatically.

---

## Part 4: What NOT to Do (Yet)

These are things from the previous plan that I'd **defer**:

| Item | Why Defer |
|------|-----------|
| `data-products/` folder hierarchy | Over-structures things you don't need yet |
| `manifest.yaml` per product | The `catalog.yaml` in metabase/ covers this more simply |
| Product catalog (`governance/catalog/`) | Your ClickUp Data Product Registry is sufficient |
| Architecture Decision Records | Good practice but not urgent — start after patterns stabilize |
| Shared Python utilities library | Only 4 processes; duplication is minimal |
| Moving governance docs from ClickUp to repo | ClickUp is where your team works; keep docs there |
| dbt | See Appendix A — adopt when you have 10+ transforms |

### When to Revisit

| Trigger | Action |
|---------|--------|
| **> 10 SQL transforms** | Adopt dbt (Dagster integrates natively — see Appendix A) |
| **Team grows beyond 3 people** | Add contribution guidelines, PR templates |
| **Multiple environments (dev/staging/prod)** | Add environment configs to Dagster |
| **Compliance requirements** | Add formal governance docs to repo |
| **Airbyte syncs need coordination** | Add Dagster sensors to trigger on Airbyte completion |

---

## Implementation Order

### Week 1: Metabase SQL (highest value, lowest risk)

1. Create `metabase/` directory and `catalog.yaml`
2. Extract SQL from the 5 native SQL models
3. Write SQL equivalent for DP-000 (Department Mapping)
4. Commit and push

### Week 2: Transforms Directory + Dagster Project Skeleton

1. Create `transforms/` directory
2. Extract `fact_active_headcount` SQL from ADP pipeline
3. Create `orchestration/` Dagster project skeleton
4. Define first asset (one silver layer transform)
5. Test locally with `dagster dev`

### Week 3: Dagster Cloud Deployment

1. Sign up for Dagster Cloud Solo Plan (30-day free trial)
2. Connect GitHub repo
3. Add DO Postgres credentials as secrets
4. Push code — verify auto-deploy works
5. Enable schedules and verify alerting
6. Shut down n8n droplet

### Week 4: Add Fraud Detection

1. Refactor fraud detection logic into SQL procs and/or Dagster Python asset
2. Add to Dagster with weekly schedule
3. Build Metabase dashboard to report on fraud detection results
4. Verify end-to-end: Dagster runs analysis → writes to Postgres → Metabase shows results

### Week 5+: Iterate

- Add more silver layer transforms as Dagster assets
- Add Metabase models to version control as you create them
- Explore Dagster sensors for ADP file automation
- Update ClickUp tasks (DSG-3308 subtasks) to reflect new plan

---

## Updating DSG-3308

The existing subtasks on DSG-3308 map roughly to this new plan:

| Old Subtask | New Action |
|------------|------------|
| Phase 1.1: Foundation Directory Structure | → Create `metabase/`, `transforms/`, `orchestration/` |
| Phase 1.2: Move ADP Headcount | → Skip (leave as-is, add Dagster sensor later) |
| Phase 1.3: Move Monthly Billing | → Skip (leave as-is) |
| Phase 1.4: Create Data Product Manifests | → Replaced by `metabase/catalog.yaml` |
| Phase 1.5: Create Product Catalog | → Keep using ClickUp registry |
| Phase 1.6: Move Framework Docs | → Skip (keep in ClickUp) |
| Phase 2.1: Governance Standards | → Defer |
| Phase 2.2: Shared Utilities | → Defer |
| Phase 2.3: Bridge Documents | → Skip |
| Phase 2.4: Update Documentation | → Add `docs/orchestration/dagster-setup.md` |
| Phase 2.5: ADRs | → Defer |
| Phase 3: GitHub Actions Orchestration | → Replace with Dagster Cloud deployment |

---

## Resolved Questions

1. **Where does the ADP file get dropped?** → Local Downloads folder, then manually copied. ADP process stays manual for now. Future option: upload to cloud storage (S3/DO Spaces) and use a Dagster sensor to detect it.
2. **For the 3 unassigned base models?** → Leave without DP-IDs for now. Assign after formal Data as a Product assessment.
3. **Hosting approach?** → Dagster Cloud Solo Plan ($10/month). Eliminates self-hosting complexity. Shut down n8n droplet.
4. **Failure alerts?** → TBD — Dagster Cloud supports email and Slack out of the box. Configure during Week 3 setup.

---

## Appendix A: dbt Migration Path

### When to Adopt dbt

dbt becomes worth the investment when:

- You have **10+ SQL transforms** with dependencies between them
- You need **built-in data quality testing** (not_null, unique, accepted_values, etc.)
- You want **auto-generated documentation** with a browsable web UI
- Multiple team members are writing transforms and need **consistent patterns**

### What Changes (and What Doesn't)

When you adopt dbt, the migration is straightforward because of how the repo is structured:

| Component | Before dbt | After dbt |
|-----------|-----------|-----------|
| SQL files | `transforms/bronze_to_silver/*.sql` | `dbt/models/silver/*.sql` (nearly identical syntax) |
| Dependencies | Defined in Dagster asset code | Defined via `{{ ref() }}` in SQL |
| Testing | Manual / custom | Built-in: `dbt test` |
| Documentation | `catalog.yaml` + comments | dbt `schema.yml` + auto-generated docs site |
| Orchestration | **Dagster (no change)** | **Dagster (no change)** |
| Scheduling | **Dagster schedules (no change)** | **Dagster schedules (no change)** |
| Monitoring | **Dagster UI (no change)** | **Dagster UI (no change)** |

The key insight: **Dagster is the constant.** It orchestrates your pipelines whether the transforms are raw SQL files, dbt models, or Python scripts. You're not throwing anything away when you add dbt — you're plugging it into the orchestration layer you've already built.

### What the Repo Would Look Like (Eventually)

```
data-platform/
├── ...                              # Everything from today
├── transforms/                      # Deprecated, replaced by dbt/
│   └── README.md                    # "Migrated to dbt/ — see that directory"
├── dbt/                             # NEW: dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── silver/
│   │   │   ├── department_mapping.sql
│   │   │   ├── fact_active_headcount.sql
│   │   │   └── schema.yml           # Tests + documentation
│   │   └── gold/
│   │       └── (business-ready marts)
│   └── tests/
│       └── (custom data quality tests)
├── orchestration/                   # Updated to use dagster-dbt
│   └── orchestration/
│       └── assets/
│           └── dbt_assets.py        # One file replaces transforms.py
└── ...
```

### The dagster-dbt Integration

Adding dbt to Dagster is remarkably simple:

```python
# orchestration/orchestration/assets/dbt_assets.py

from dagster_dbt import DbtCliResource, dbt_assets
from pathlib import Path

dbt_project_dir = Path(__file__).parents[3] / "dbt"

@dbt_assets(manifest=dbt_project_dir / "target" / "manifest.json")
def my_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
```

That single function imports every dbt model as a Dagster asset — with full lineage, test results, and documentation visible in the Dagster UI. Your Python assets (fraud detection, ADP processing) continue to work alongside dbt assets in the same DAG.

### Cost of dbt

- **dbt Core:** Free, forever, open source
- **No new server needed:** Runs within your Dagster Cloud serverless compute — no additional infrastructure
- **dbt Cloud:** Not needed. Dagster replaces everything dbt Cloud provides (scheduling, monitoring, CI)

### Not Now, But Keep It in Mind

You don't need dbt today. But knowing the migration path lets you make decisions now that won't create rework later. Specifically:

- Write transforms as **standalone SQL files** (not embedded in Python) — these become dbt models with minimal changes
- Use **Dagster** for orchestration — it's the one piece that survives the dbt migration unchanged
- Keep SQL **in the repo** — version-controlled SQL is the foundation of both approaches
