# `orchestration/` — Dagster Cloud Project

This is the Dagster project that orchestrates everything in the data platform —
report runs, ELT, transforms (when migrated), data-quality checks. It is deployed
to **Dagster Cloud (Solo Plan)** and auto-deploys on push to `main`.

For now it contains a single asset group — `daily_reports` — wrapping the
recruiter-activity report. As the data platform migrates more workloads to
Dagster, additional asset groups will join this project (transforms, ELT
sensors, etc., per the plan in `../REPO_RECOMMENDATIONS.md` Part 3).

## Read first

- **[../reporting/routines/_framework.md § 14](../reporting/routines/_framework.md)** —
  how reports register as Dagster assets.
- **[../REPO_RECOMMENDATIONS.md Part 3](../REPO_RECOMMENDATIONS.md)** — the
  Dagster Cloud architecture decision and migration plan.

## Layout

```
orchestration/
├── pyproject.toml                  ← Dagster project config (module_name, etc.)
├── setup.py                        ← editable-install shim
├── README.md                       ← you are here
└── orchestration/                  ← the Dagster project package
    ├── __init__.py
    ├── definitions.py              ← top-level Definitions(...)
    ├── schedules.py                ← cron schedules
    ├── assets/
    │   └── reports/                ← thin wrappers around Report.run()
    │       └── recruiter_activity.py
    └── resources/                  ← shared resources (placeholder for now)
```

## Local development

```bash
# From the repo root, install both packages in editable mode
pip install -e ./reporting
pip install -e ./orchestration

# Boot the Dagster UI (defaults to http://localhost:3000)
cd orchestration
dagster dev
```

In another terminal:

```bash
# Materialize the asset on demand (skips the schedule)
dagster asset materialize --select recruiter_activity_report
```

The asset will fail until the framework code in `../reporting/src/reporting/` is
implemented — the stubs raise `NotImplementedError`. Local boot of `dagster dev`
should still succeed (Dagster only imports the asset definitions, not their bodies).

## Production deployment

See `../reporting/MIGRATION.md` for the Dagster Cloud onboarding checklist. In
short:

1. Sign up Dagster Cloud Solo Plan.
2. Connect this GitHub repo as the code location.
3. Configure Dagster Cloud secrets (DATABASE_URL, POWER_AUTOMATE_URL,
   ANTHROPIC_API_KEY).
4. Decide Hybrid agent vs. Dagster-managed agent (depends on warehouse network
   reachability — see open question in `recruiter-activity.md` § 11).
5. Push to `main` — Dagster Cloud picks up the new code.
6. Enable the schedule(s) in the Dagster Cloud UI.
