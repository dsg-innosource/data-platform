# Reporting Framework Migration Tracker

Living document that tracks the state of the reporting framework migration.
Every commit that changes the migration state should also update this file.

**Status:** Phase 1 complete (specs + skeleton in repo). Phase 2 (implementation)
not started.

**Last updated:** 2026-04-27

---

## Overview

We are migrating three production reports from interactive Cowork prompts to a
shared, DRY framework that runs as Dagster Cloud assets. Reports keep their
LLM-generated insights via Anthropic API narrative refinement, with deterministic
fallback text for reliability.

```
Cowork interactive prompts          →    data-platform reporting framework
(prompts/reporting/*.md)                 (reporting/ + orchestration/)

Manual trigger in Cowork chat       →    Dagster Cloud schedule
Pattern B browser-fetch email       →    requests.post directly
Hardcoded recipients in scripts     →    config/recipients.md, parsed at runtime
LLM regenerates HTML each run       →    Jinja2 templates + LLM refines insights only
Three reports, three styles         →    One framework, three thin Report subclasses
```

---

## Phase 1 — Specification + Skeleton (DONE)

| Deliverable | State | Location |
|-------------|-------|----------|
| Framework spec | DONE | `routines/_framework.md` |
| Recruiter-activity routine spec | DONE | `routines/recruiter-activity.md` |
| Routines README | DONE | `routines/README.md` |
| Top-level reporting README | DONE | `README.md` |
| `reporting/` Python package skeleton | DONE | `src/reporting/` (8 framework modules + 1 report stub) |
| `reporting/` pyproject.toml | DONE | `pyproject.toml` |
| Style guide vendored from cowork | DONE | `styles/inno_source_report_style_guide.md` |
| Recipients config vendored from cowork | DONE | `config/recipients.md` |
| HTML-to-PDF tool vendored from cowork | DONE | `tools/html-to-pdf/` |
| Dagster orchestration project | DONE | `../orchestration/` |
| Recruiter-activity Dagster asset | DONE (stub) | `../orchestration/orchestration/assets/reports/recruiter_activity.py` |
| Recruiter-activity schedule | DONE (stub) | `../orchestration/orchestration/schedules.py` |
| `requirements.txt` updated | DONE | `../requirements.txt` |
| `README.md` updated | DONE | `../README.md` |
| `CLAUDE.md` updated | DONE | `../CLAUDE.md` |
| `REPO_RECOMMENDATIONS.md` addendum | DONE | `../REPO_RECOMMENDATIONS.md` |

**What "skeleton" means here.** All files exist with type-annotated public
interfaces and docstrings. All implementation methods raise `NotImplementedError`.
The Dagster project boots and shows the asset in the lineage UI; the asset
fails on materialization because the framework is not yet implemented.

---

## Phase 2 — Implementation (NEXT)

Order matters. Each step is testable on its own.

### 2a. Framework implementation

| Module | What to implement | Spec ref |
|--------|-------------------|---------|
| `framework/window.py` | `report_window(today)` — Monday rule | `_framework.md § 4` |
| `framework/db.py` | `connect()` reading `shared/database.yaml` | `_framework.md § 10` |
| `framework/config.py` | `recipients_for(section_name)` parsing markdown table; `env()` helper | `_framework.md § 8`, `§ 10` |
| `framework/style.py` | `base_css()` extracting from style guide markdown | `_framework.md § 1` |
| `framework/render.py` | Jinja2 setup + `html_to_pdf()` subprocess wrapper | `_framework.md § 1` |
| `framework/email.py` | `send()` — single `requests.post` to Power Automate | `_framework.md § 11` |
| `framework/guards.py` | Five-rule guard + `EmailGuardError` | `_framework.md § 6` |
| `framework/narrative.py` | Anthropic SDK call with timeout + JSON-schema validation + fallback | `_framework.md § 5` |
| `framework/base.py` | `Report.run()` — orchestrates all steps | `_framework.md § 2` |
| `cli.py` | argparse → Report.run() | `_framework.md § 9` |

### 2b. Recruiter-activity report

| Module | What to implement | Spec ref |
|--------|-------------------|---------|
| `reports/recruiter_activity/queries.py` | Paste SQL from legacy prompt | `recruiter-activity.md § 4` |
| `reports/recruiter_activity/callouts.py` | Implement rule table | `recruiter-activity.md § 7` |
| `reports/recruiter_activity/report.py` | Wire fetch / derive_callouts / render_* | `recruiter-activity.md § 8` |
| `reports/recruiter_activity/templates/pdf.html.j2` | Lift from legacy "HTML Page Skeleton" | `recruiter-activity.md § 8` |
| `reports/recruiter_activity/templates/email.html.j2` | Lift from legacy "Step 7a — Build email body" | `recruiter-activity.md § 8` |

### 2c. Tests

| Test file | What to cover |
|-----------|---------------|
| `tests/framework/test_window.py` | Monday rule, year boundary, leap day, DST |
| `tests/framework/test_guards.py` | Each of the 5 guard rules, positive + negative |
| `tests/framework/test_email.py` | Power Automate payload shape (mocked HTTP) |
| `tests/framework/test_config.py` | Recipient parsing + count-mismatch detection |
| `tests/reports/test_recruiter_activity_callouts.py` | Each rule fires on its threshold; severity precedence |
| `tests/reports/test_recruiter_activity_render.py` | Style-guard assertions on rendered HTML |

### 2d. Local validation

```bash
# Install both packages editable
pip install -e ./reporting -e ./orchestration

# Window sanity
python -m reporting.cli recruiter-activity --window-only

# Dry run — render files, do not send
python -m reporting.cli recruiter-activity --dry-run

# Send to Sean only
python -m reporting.cli recruiter-activity --send-to sbeal@innosource.com

# Validate Anthropic fallback
NARRATIVE_DISABLE=1 python -m reporting.cli recruiter-activity \
    --send-to sbeal@innosource.com

# Run tests
pytest reporting/tests/ -v

# Boot Dagster locally
cd orchestration && dagster dev
# In another terminal:
dagster asset materialize --select recruiter_activity_report
```

**Acceptance for Phase 2:** all of the above succeed, the email lands in Sean's
inbox, and the PDF visually matches a recent production PDF on hand (a copy
emailed to a recipient in the past week works as a fidelity reference).

---

## Phase 3 — Dagster Cloud Cutover (AFTER Phase 2)

This phase has both code work and human/account work. Code work is small;
human work has prerequisites.

### 3a. Dagster Cloud account setup (Sean — manual)

1. Sign up Dagster Cloud Solo Plan ($10/month) at https://dagster.cloud/.
2. Connect the GitHub repo `dsg-innosource/data-platform` as a code location.
3. Decide: **Hybrid agent** (runs in our infra, can reach private DB) vs.
   **Dagster-managed agent** (runs in Dagster's infra; warehouse must accept
   public connections from Dagster's egress IP).

   The warehouse is on Digital Ocean. If it's not currently public-IP-restricted
   to our team's addresses, the Dagster-managed agent works. If it is, we need
   the Hybrid agent. Check with whoever owns the DO Postgres networking rules.

4. Configure secrets in Dagster Cloud (Workspace settings → Secrets):
   - `DATABASE_URL`
   - `POWER_AUTOMATE_URL`
   - `ANTHROPIC_API_KEY` (recommend a separate API key scoped to this workload)
   - `NARRATIVE_MODEL` (optional override)

### 3b. Code work

- [ ] Confirm `pyproject.toml` `tool.dagster.module_name` and
      `code_location_name` match what Dagster Cloud expects.
- [ ] Add a `.github/workflows/dagster-deploy.yml` that triggers Dagster Cloud
      build-from-branch on push to `main`. (Dagster Cloud provides a templated
      action — paste it once.)
- [ ] Add a `.github/workflows/python-validation.yml` running `pytest reporting/tests/`
      so report rule changes are CI-validated. Parallel to existing `sql-validation.yml`.

### 3c. Cutover

1. Push to `main` — Dagster Cloud auto-deploys.
2. Verify the asset appears in Dagster Cloud UI.
3. Manually materialize once — sends to Sean only (override is hardcoded in the
   asset for the first cutover run). Confirm the email and PDF look right.
4. Remove the override from the asset, push again.
5. Enable the schedule in the Dagster Cloud UI.
6. Set up failure alerting: Dagster Cloud → Settings → Alerts → on Asset
   Failure → Slack webhook or email distribution.

### 3d. Cleanup

- [ ] Archive `cowork/scripts/position-status-gen/generate_*.py` (the dated
      iterations). Keep the most recent `generate.py` for reference.
- [ ] Add a deprecation banner to the cowork interactive prompts pointing at
      this repo.

---

## Phase 4 — Migrate the Other Two Reports

Once recruiter-activity is running cleanly in Dagster Cloud:

1. **terminations** — weekly report, ISO-week window. Override
   `Report.report_window()` for the weekly logic. Otherwise structurally
   similar to recruiter-activity.
2. **position-status** — refactor of existing `cowork/scripts/position-status-gen/generate.py`
   into framework shape. Largest visual surface, but already deterministic, so
   the lift is mostly mechanical.

Each gets its own routine spec at `routines/<name>.md`, package at
`src/reporting/reports/<name>/`, tests, and Dagster asset wrapper.

---

## Open Questions (track and resolve as we go)

| # | Question | Owner | Status |
|---|----------|-------|--------|
| Q1 | Hybrid agent vs. Dagster-managed agent? Depends on warehouse network rules. | Sean | Open — resolve in Phase 3a |
| Q2 | Separate Anthropic API key for prod, or reuse Sean's? | Sean | Open — recommendation: separate key for cost tracking + rotation independence |
| Q3 | DP-ID range for reports — `DP-021`–`DP-040` reserved? | Sean | Open — confirm against current `metabase/catalog.yaml` next-available ID |
| Q4 | Power Automate webhook URL stability — does the SAS-style `sig=` parameter rotate? | Sean / IT | Open — set up a 401/403 alert regardless |
| Q5 | Should `cowork/config/alert-recipients.md` be deleted once `reporting/config/recipients.md` is canonical, or kept for the legacy prompts? | Sean | Open — recommendation: keep both during Phase 2/3, delete cowork copy in Phase 4 cleanup |

---

## What's Still in Cowork (intentionally)

These stay in `cowork/` because they support interactive Claude prompt use, which
remains a useful debugging path:

- `prompts/reporting/*.md` — interactive Claude prompts for ad-hoc runs
- `prompts/data-quality/`, `prompts/hr-ops/` — unrelated to reporting
- `outputs/` — scratch space for local generation runs
- `workflows/*.md` — Cowork-specific workflow notes (Pattern B, etc.)
- Dated artifacts (`activity-2026-04-23.html`, etc.) — historical record

The cowork copies of `config/style/`, `config/alert-recipients.md`, and
`scripts/html-to-pdf/` are kept in place during Phase 2/3 so the legacy
interactive prompts continue to work. Once production has fully cut over via
Dagster Cloud, the cowork copies can be deleted in Phase 4 cleanup.
