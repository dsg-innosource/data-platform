# `reporting/routines/` — Production Reporting Specs

This directory holds the **specs** for InnoSource's production reporting routines.
The implementation lives one level up at `reporting/framework/` (shared lifecycle)
and `reporting/reports/<name>/` (per-report code). Production scheduling lives in
`orchestration/` (Dagster Cloud).

## Read order

1. **[`_framework.md`](./_framework.md)** — start here. Describes the shared lifecycle,
   data structures, narrative integration, recipient management, environment
   variables, Dagster asset registration, and DP-ID assignment. Every per-report
   spec inherits from this.
2. **[`recruiter-activity.md`](./recruiter-activity.md)** — the daily recruiter
   activity report. First report being migrated to the framework.
3. *(planned)* `terminations.md` — weekly terminations entered report.
4. *(planned)* `position-status.md` — daily position status report (refactor of the
   existing `cowork/scripts/position-status-gen/generate.py`).

## Relationship to `cowork/prompts/reporting/`

| Location | Purpose | Audience |
|----------|---------|----------|
| `cowork/prompts/reporting/*.md` | Interactive Cowork prompts that Claude executes step-by-step | Humans triggering ad-hoc runs in chat; debugging |
| `data-platform/reporting/routines/*.md` | Production specs for the framework-driven implementation | Humans + future Claude sessions reasoning about prod behavior |
| `data-platform/reporting/framework/`, `data-platform/reporting/reports/<name>/` | Implementation | Dagster Cloud, CI, future engineers |
| `data-platform/orchestration/orchestration/assets/reports/*.py` | Dagster asset wrappers that schedule and monitor each report | Dagster Cloud |

The interactive prompts in `cowork/prompts/reporting/` continue to work — they are
the fastest way to debug a report against today's data. Production runs and the
schedules in Dagster Cloud flow through this framework instead.

## Migration order

`recruiter-activity` → `terminations` → `position-status`. The position-status
refactor is last because the existing Python generator
(`cowork/scripts/position-status-gen/generate.py`) is the most complex of the three;
we want the framework battle-tested on the simpler reports first.

## Quick reference — running a routine

```bash
# Local development (CLI)
python -m reporting.cli recruiter-activity --dry-run
python -m reporting.cli recruiter-activity --send-to sbeal@innosource.com

# Local Dagster
cd orchestration && dagster dev
dagster asset materialize --select recruiter_activity_report

# Production — runs automatically on schedule via Dagster Cloud.
# Manual trigger via Dagster Cloud UI.
```

See `_framework.md` § 9 for the full CLI surface and § 14 for the Dagster asset
pattern.
