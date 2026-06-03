# `reporting/` — InnoSource Production Reporting Framework

Daily and weekly operational reports — the reports that get emailed to
operations leaders every morning. This package is the **production**
implementation; it queries the warehouse, applies deterministic callout rules,
optionally refines the wording via the Anthropic API, renders an HTML report
plus an inline-styled email body, converts the report to a PDF, and sends both
through the Power Automate email webhook.

Production scheduling, monitoring, and retries are owned by **Dagster Cloud**
(see `../orchestration/`). This package is invoked from a Dagster asset; it is
also runnable standalone via the CLI for local development.

## Read this first

- **[routines/_framework.md](routines/_framework.md)** — shared lifecycle,
  data structures, narrative integration, recipient management, environment
  variables, Dagster registration, DP-ID assignment.
- **[routines/recruiter-activity.md](routines/recruiter-activity.md)** — the first
  report being migrated, with rule tables and template inputs.
- **[MIGRATION.md](MIGRATION.md)** — phase tracker, what's stubbed vs. implemented,
  human action items.

## Layout

```
reporting/
├── pyproject.toml                  ← installable as the `reporting` package
├── README.md                       ← you are here
├── MIGRATION.md                    ← phase tracker
├── routines/                       ← spec markdown
├── src/reporting/                  ← Python package (src layout)
│   ├── cli.py
│   ├── framework/                  ← shared lifecycle (window, db, render, email,
│   │   │                              narrative, guards, config, style, base)
│   └── reports/                    ← per-report packages
│       └── recruiter_activity/
├── styles/                         ← brand CSS source-of-truth
├── tools/html-to-pdf/              ← Node Playwright PDF tool
├── config/                         ← runtime config (recipients.md)
└── tests/
```

## Local development

```bash
# From the repo root
pip install -e ./reporting

# Sanity check
python -m reporting.cli recruiter-activity --window-only

# Dry run — render files, do not send
python -m reporting.cli recruiter-activity --dry-run

# Send to one address (dev mode — subject is prefixed with [DEV])
python -m reporting.cli recruiter-activity --send-to sbeal@innosource.com
```

Required env vars (load via `.env` at the repo root):

```
DATABASE_URL=postgresql://...
POWER_AUTOMATE_URL=https://...
ANTHROPIC_API_KEY=sk-...
```

## Production

Reports are scheduled and run by Dagster Cloud. See `../orchestration/` for the
asset definitions, schedules, and resource configuration. Each report's asset is
a thin wrapper that calls `Report.run()` and emits `RunResult` as the asset
materialization output, so it shows up in the Dagster lineage view.
