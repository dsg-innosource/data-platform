---
title:        Reporting Routine Framework
version:      0.2
author:       Sean Beal
status:       draft — Phase 1 spec
last_updated: 2026-04-27
applies_to:   recruiter-activity, position-status, terminations
orchestrator: Dagster Cloud (Solo Plan)
---

# Reporting Routine Framework

This document is the **shared spec** that every InnoSource production report inherits
from. Each individual report (`recruiter-activity.md`, `position-status.md`,
`terminations.md`) describes only what is **unique** to that report — its SQL,
thresholds, callout rules, and email layout. Everything else — date windows, narrative
generation, PDF rendering, email transport, recipient management, dry-run behavior,
file naming, error handling, scheduling — is defined here, once.

> **Why this exists.** Before this framework, each report was its own snowflake:
> different window logic, different email send code, different output paths. That made
> three reports feel like nine. Standardizing the lifecycle means a fourth report is
> ~200 lines of work, not 2,000.

> **Why these reports live in `data-platform/`.** Reports query views defined in
> `data-platform/database/views/silver/`. When a view's schema changes, the report
> should change in the same PR. The Data as a Product framework already in this repo
> (DP-IDs, `metabase/catalog.yaml`) extends to reports as a first-class data product
> type — not just dashboards.

---

## 1. Implementation home

Everything lives under `data-platform/`:

```
data-platform/
├── reporting/                      ← this framework + per-report packages
│   ├── routines/                   ← spec markdown (this document)
│   │   ├── _framework.md           ← shared lifecycle (you are here)
│   │   ├── recruiter-activity.md
│   │   ├── position-status.md      ← (planned)
│   │   └── terminations.md         ← (planned)
│   ├── framework/                  ← shared implementation
│   │   ├── __init__.py
│   │   ├── base.py                 ← Report ABC and run() lifecycle
│   │   ├── window.py               ← report_window(today) — Monday rule, week alignment
│   │   ├── db.py                   ← psycopg2 connection helper (uses shared/database.yaml)
│   │   ├── render.py               ← Jinja2 → HTML → PDF (wraps tools/html-to-pdf/convert.js)
│   │   ├── email.py                ← Power Automate webhook client (single requests.post)
│   │   ├── narrative.py            ← Anthropic API narrative refinement, with fallback
│   │   ├── guards.py               ← pre-send body checks (HTML comments, bash artifacts)
│   │   ├── config.py               ← recipients, secrets, env loading
│   │   └── style.py                ← shared CSS loader (styles/)
│   ├── reports/                    ← one package per report
│   │   ├── recruiter_activity/
│   │   │   ├── __init__.py
│   │   │   ├── report.py           ← subclass of Report
│   │   │   ├── queries.py          ← parameterized SQL + executor
│   │   │   ├── callouts.py         ← deterministic rules → list[Callout]
│   │   │   └── templates/
│   │   │       ├── pdf.html.j2
│   │   │       └── email.html.j2
│   │   ├── position_status/        ← refactor of scripts/position-status-gen/generate.py
│   │   └── terminations/
│   ├── styles/                     ← brand CSS source-of-truth
│   │   └── inno_source_report_style_guide.md
│   ├── tools/
│   │   └── html-to-pdf/            ← Node Playwright PDF tool
│   ├── config/
│   │   └── recipients.md           ← named distribution lists
│   ├── tests/
│   │   ├── framework/              ← test_window.py, test_guards.py, test_email.py
│   │   └── reports/                ← per-report golden tests for callouts + render
│   ├── pyproject.toml              ← installable as the `reporting` package
│   ├── README.md
│   └── MIGRATION.md                ← phase tracker
└── orchestration/                  ← Dagster Cloud project
    ├── orchestration/
    │   ├── definitions.py          ← top-level Definitions(...)
    │   ├── assets/
    │   │   └── reports/            ← thin Dagster wrappers around Report.run()
    │   │       ├── recruiter_activity.py
    │   │       ├── position_status.py
    │   │       └── terminations.py
    │   ├── resources/              ← shared resources (DB, Anthropic client)
    │   └── schedules.py            ← cron schedules per report
    ├── pyproject.toml
    └── setup.py
```

The Node Playwright PDF tool lives in this repo at `reporting/tools/html-to-pdf/`.
The data-platform repo is fully self-contained for production — nothing in
`reporting/` or `orchestration/` depends on files outside this repository.

---

## 2. The Report lifecycle

Every report follows this exact lifecycle. The `Report.run()` method in
`framework/base.py` is the only place that orchestrates these steps — individual
reports cannot reorder them.

| Step | Method on Report subclass | What it does |
|------|---------------------------|--------------|
| 1 | (framework) | Resolve `Window` from today's date (Monday rule applied) |
| 2 | (framework) | Open DB connection from `DATABASE_URL` |
| 3 | `fetch(conn, window)` | Run report-specific queries, return `ReportData` |
| 4 | `derive_callouts(data)` | Apply deterministic rule table, return `list[Callout]` |
| 5 | (framework) | Optional: refine callout wording via Anthropic API (`narrative.refine()`) |
| 6 | `render_pdf_html(data, callouts)` | Jinja2 → full PDF-style HTML page |
| 7 | `render_email_html(data, callouts)` | Jinja2 → email body HTML |
| 8 | (framework) | Pre-send guards on email body (raises on violation) |
| 9 | (framework) | Convert PDF HTML → PDF via `tools/html-to-pdf/convert.js` |
| 10 | (framework) | Send via Power Automate webhook (or skip if `dry_run=True`) |
| 11 | (framework) | Return `RunResult` (Dagster materializes this; logs are captured by Dagster) |

Anything report-specific lives in steps 3, 4, 6, and 7. Everything else is shared.

When triggered by Dagster (production), the asset wrapper instantiates the report
and calls `run()`. When triggered locally (CLI), `python -m reporting.cli <name>`
does the same thing.

---

## 3. Standard data structures

```python
@dataclass(frozen=True)
class Window:
    today: date                  # the date the run was triggered
    report_date: date            # the date the report covers (Monday rule applied)
    window_start: date           # inclusive
    window_end: date             # inclusive
    label_long: str              # "Friday's Activity · Fri, March 20, 2026"
    label_short: str             # "March 20, 2026"
    is_monday_run: bool

@dataclass
class Callout:
    section: str                 # "section-02", "section-03", "insights", etc.
    tone: Literal["positive", "watch", "context", "alert"]
    rule_id: str                 # stable id for the deterministic rule that fired
    fallback_text: str           # rule-table-derived text — used if narrative fails
    refined_text: str | None     # Claude-refined text — populated in step 5
    data: dict                   # raw values referenced in the text (for templates)

@dataclass
class ReportData:
    window: Window
    sections: dict[str, Any]     # report-specific

@dataclass
class RunResult:
    routine_name: str
    window: Window
    pdf_path: Path
    email_html_path: Path
    sent: bool
    recipient_count: int
    narrative_used: bool         # true if Claude refinement succeeded
    duration_seconds: float
```

The `Callout` separation is the core of the narrative-integration strategy. Step 4
**always** populates `fallback_text` from the rule table — that is the safety net.
Step 5 attempts to populate `refined_text`; if it fails for any reason, templates
render `refined_text or fallback_text`.

`RunResult` is what Dagster sees as the asset materialization output — it shows up
in the lineage view and run history.

---

## 4. Date window logic — Monday rule

The Monday rule applies to all daily reports (recruiter-activity, position-status):
on Monday the report covers Friday, with the query window expanding to Friday + Sat +
Sun in case any weekend entries exist. Tue–Fri the report covers the prior calendar
day.

This is implemented **once** in `framework/window.py::report_window(today)` and used
by every daily report. Weekly reports (terminations) override `report_window()` to
return an ISO-week-aligned window instead.

```python
def report_window(today: date) -> Window:
    if today.weekday() == 0:  # Monday
        report_date = today - timedelta(days=3)
        window_start = report_date
        window_end = today - timedelta(days=1)  # Sunday
        return Window(
            today=today, report_date=report_date,
            window_start=window_start, window_end=window_end,
            label_long=f"Friday's Activity · Fri, {report_date:%B %-d, %Y}",
            label_short=f"{report_date:%B %-d, %Y}",
            is_monday_run=True,
        )
    else:
        d = today - timedelta(days=1)
        return Window(
            today=today, report_date=d,
            window_start=d, window_end=d,
            label_long="Today's Activity",
            label_short=f"{d:%B %-d, %Y}",
            is_monday_run=False,
        )
```

If a report needs a different window (e.g. terminations covers an ISO week), it
overrides the framework's `report_window()` method.

---

## 5. Narrative integration — how Claude stays in the loop

The "insights" sections in every report are LLM-generated. This is the primary reason
we are using a hybrid Python+Claude architecture instead of pure Metabase. The
integration is a single function: `framework.narrative.refine(callouts, data)`.

### Contract

**Input to Claude:**

```json
{
  "report_name": "recruiter-activity",
  "brand_voice": "<prompt fragment from framework — see below>",
  "report_window": {"label_long": "...", "label_short": "...", "is_monday_run": true},
  "data": { "...report-specific raw numbers..." },
  "callouts": [
    {
      "rule_id": "oar-strong",
      "section": "insights",
      "tone": "positive",
      "fallback_text": "Acceptance rate hit 95% on 12 of 12 offers.",
      "data": {"oar": 95.0, "accepted": 12, "extended": 12}
    }
  ]
}
```

**Output from Claude (strict JSON schema):**

```json
{
  "callouts": [
    {"rule_id": "oar-strong", "refined_text": "<1–3 sentence refined version>"}
  ]
}
```

The refiner **may not invent new callouts** — it only refines wording for callouts the
deterministic rules already produced. This keeps the rules table as the single source
of truth for which insights surface.

### Implementation

- Use the **Anthropic Python SDK directly** (`anthropic` package), not `claude -p` and
  not the Claude Code CLI. This keeps the Dagster runner lean — just `pip install
  anthropic` and an API key in Dagster Cloud secrets.
- Model: `claude-sonnet-4-6` (current production tier). Make this configurable via
  `NARRATIVE_MODEL` env var so we can fall back to Haiku for cost optimization later.
- Timeout: 30 seconds. On timeout/error/schema-validation failure, **silently fall
  back** to `fallback_text` — never block the report from going out.
- Log every call (prompt, response, latency, fell-back-or-not) via Dagster's logger
  so it appears in the run log; also append to a structured asset metadata field
  on `RunResult` so we can query historical narrative usage.
- Disable narrative refinement entirely with `NARRATIVE_DISABLE=1` (used for testing
  determinism, or as a kill switch if the API is down).

### Brand voice (canonical)

This text goes verbatim into the system prompt for `narrative.refine`:

> You are writing one-paragraph insights for an InnoSource operational report. Style:
> plain, direct business language. Lead with the insight. No filler phrases ("It's
> worth noting that…", "Of particular interest…"). No metaphors. No exclamation
> points. Numbers belong in the sentence, not parenthetically. 1–3 sentences per
> callout, never more. The reader is an experienced operations leader who has 30
> seconds — assume competence.
>
> You are refining wording only. The rule that triggered each callout already decided
> *what* to say; your job is *how* to say it. Do not invent new callouts. Do not
> remove callouts. Do not change the tone classification.

---

## 6. Pre-send guards (mandatory)

Before any email is sent, `framework.guards.check_email_body(html)` runs and raises
`EmailGuardError` (caught by the Dagster asset and surfaced as a failure) on any of:

1. **HTML comments** — `<!--` or `-->` anywhere in the body. Outlook and iOS Mail
   render them as visible text. (This guard exists because of an April 2026
   incident where an HTML comment in the body shipped to all 10 production
   recipients; the framework's templates and tests are designed to never produce
   them in the first place, but this is a cheap belt-and-suspenders backstop.)
2. **Bash heredoc artifacts** — `\!` anywhere. (Same incident: bash
   history-expanded `<!--` to `<\!--` when the body was assembled via heredoc.
   The framework writes templates via Jinja2, not bash, so this guard is unlikely
   to fire — but it's free insurance.)
3. **Empty body** — `len(body.strip()) < 100` characters.
4. **Unrendered Jinja markers** — `{{` or `}}` or `{%` anywhere. (Catches template
   bugs.)
5. **Unsubstituted placeholders** — `[PLACEHOLDER]` or `[any_word]` patterns common
   to copy-paste mistakes (with an allowlist for legitimate uses like `[N of N]`).

These guards run **on the rendered string**, not the template. They are the same on
every report.

---

## 7. Output paths

When run from Dagster, files are written to a per-run directory under
`/tmp/dagster-reports/<routine>/<run-id>/` and the PDF is uploaded as a Dagster asset
output. The PDF is also attached to the outbound email (no separate storage needed —
the email recipient list is the durable record).

When run locally (CLI), files go to `<repo-root>/.report-out/` (gitignored).

| File | Path | Purpose |
|------|------|---------|
| PDF source HTML | `{out_dir}/{routine}-{YYYY-MM-DD}.html` | The styled report page (PDF input) |
| Email body HTML | `{out_dir}/{routine}-email-{YYYY-MM-DD}.html` | Inline-styled email body |
| PDF | `{out_dir}/{routine}-{YYYY-MM-DD}.pdf` | The attached PDF |

For weekly reports, substitute `week-{NN}-{YYYY}` for `{YYYY-MM-DD}` in the file
names. The routine spec specifies this.

---

## 8. Recipient management

Recipients live **only** in `reporting/config/recipients.md` — never inline in code or
templates. Each routine has a named section in that file. The section name does not
have to match the routine name (legacy sections use the `-report` suffix); the per-
report spec declares the section name explicitly via the Report subclass attribute
`recipients_section_name`.

`framework.config.recipients_for(section_name)` parses the markdown table for that
section and returns a list of email addresses. The framework joins them with `;` for
Power Automate.

### Verification on every run

Before sending, the framework counts the addresses in the joined string and counts
the rows in the markdown table. **If they do not match, abort the send and raise.**
This was a manual check in the old prompts; it's now automatic.

### Override for testing

Pass `recipients_override=[...]` to `Report.run()` (CLI: `--send-to addr1,addr2`) to
bypass the config file. The override is logged and shows in the email subject as
`[DEV]` so a misrouted test is obvious.

---

## 9. CLI surface (local development)

```
python -m reporting.cli <routine-name> [flags]

flags:
  --date YYYY-MM-DD     Override "today" — useful for backfilling or testing
  --dry-run             Render files but do not send email
  --send-to LIST        Comma-separated override recipients (adds [DEV] to subject)
  --no-narrative        Skip Claude narrative refinement; use fallback text only
  --window-only         Print computed window and exit (sanity check)
```

In production (Dagster Cloud), the same code runs but the entry point is the asset
materialization — see § 14.

**Exception classes (raised by framework, caught by Dagster asset):**
- `DBConnectionError` — connection or query failure
- `EmailGuardError` — guard violation
- `RecipientCountError` — table-vs-joined mismatch
- `WebhookError` — Power Automate returned non-200
- `PDFConversionError` — Node script failed

These are subclasses of `ReportRunError`. Dagster surfaces the failure with the
exception class name in the run log.

---

## 10. Environment variables

In production these come from **Dagster Cloud secrets** (set once in the Dagster
project settings; available to all asset runs).

| Var | Required | Purpose |
|-----|----------|---------|
| `DATABASE_URL` | yes | Postgres connection string for the warehouse (matches `shared/database.yaml` env vars when expanded — or use that yaml directly via `framework.db`) |
| `POWER_AUTOMATE_URL` | yes | Send-alert-email webhook URL |
| `ANTHROPIC_API_KEY` | only if narrative enabled | For `narrative.refine()` |
| `NARRATIVE_MODEL` | no | Override Claude model; defaults to `claude-sonnet-4-6` |
| `NARRATIVE_DISABLE` | no | Set to `1` to skip narrative entirely |

For local development, these are read from the project-level `.env` (gitignored, same
as the existing data-platform processes use).

The framework deliberately reuses `shared/database.yaml` for DB credentials so we
inherit the existing data-platform connection pattern instead of a parallel one.

---

## 11. Local-vs-Dagster-Cloud differences (intentionally minimal)

| Concern | Local (CLI / Cowork) | Production (Dagster Cloud) |
|---------|----------------------|----------------------------|
| Trigger | `python -m reporting.cli recruiter-activity` | Dagster asset materialization (scheduled) |
| Secrets source | `.env` file at repo root | Dagster Cloud secrets manager |
| Logging | stdout / stderr | Dagster run log + asset metadata |
| Failure alerting | exit code (developer sees it) | Dagster Cloud alert hooks (Slack/email) |
| Retry on failure | manual rerun | Dagster `RetryPolicy` (3x with backoff) |
| Auto-deploy | `git pull` | Dagster Cloud watches GitHub `main` |
| DB access | psycopg2 from local network | psycopg2 from Dagster Cloud agent (or hybrid agent in our infra) |
| PDF render | local Node + Playwright | Dagster Cloud agent has Node + Playwright in image |

The two environments run **identical Python code**. Anything that would differ goes
through `framework.config` (e.g. where to write tmp files, how to read secrets) so
the Report subclass never has to care.

> **Hybrid agent note.** Dagster Cloud Solo Plan supports running the agent
> in-customer infrastructure ("Hybrid"). If the warehouse is on a private network and
> doesn't accept public connections, the Hybrid agent is required. This is a Phase 3
> decision — see `MIGRATION.md`.

---

## 12. Migration plan from existing `cowork/prompts/reporting/*.md`

The existing markdown files in `cowork/prompts/reporting/` are **interactive specs**
that Claude executes step-by-step in Cowork. They will continue to work for ad-hoc
and debugging runs, but production runs flow through the framework instead.

For each report, the migration is:

1. **Spec it** — write `data-platform/reporting/routines/<name>.md` (per-report spec).
   Reference the legacy prompt file for visual layout but do not copy-paste the SQL
   or callout rules.
2. **Implement it** — build `data-platform/reporting/reports/<name>/` package with
   `report.py`, `queries.py`, `callouts.py`, and the two Jinja templates.
3. **Test it locally** — run with `--dry-run`, then `--send-to sbeal@innosource.com`,
   compare PDFs against last week's production output.
4. **Wrap it in Dagster** — add an asset in
   `data-platform/orchestration/orchestration/assets/reports/<name>.py` plus a
   schedule. Test via `dagster dev` locally.
5. **Cut over** — push to GitHub `main` (Dagster Cloud auto-deploys), enable the
   schedule in Dagster Cloud UI, archive the legacy interactive prompt or leave it
   in place as a debug-mode reference.

Order: `recruiter-activity` first (simplest, shortest report, least visual
complexity). Then `terminations`. Then `position-status` (refactor of existing
`cowork/scripts/position-status-gen/generate.py` into framework shape — most visual
complexity but already deterministic, so the lift is mostly mechanical).

---

## 13. Testing strategy

Each report package includes:

- **`tests/reports/test_<name>_callouts.py`** — golden tests for the deterministic
  rule table. For each rule, a fixture of input data → expected `Callout`. Adding a
  rule **requires** adding a fixture.
- **`tests/reports/test_<name>_render.py`** — render the templates against a fixture
  data set and assert the rendered HTML contains expected strings. Catches Jinja
  typos before they ship.
- **`tests/framework/test_window.py`** — Monday rule, week boundaries, edge cases
  (DST, year boundary, leap day).
- **`tests/framework/test_guards.py`** — every guard rule has positive and negative
  fixtures.
- **`tests/framework/test_email.py`** — Power Automate payload shape (mocked HTTP).

The narrative refinement step is **not** unit-tested against the live API — it has a
mocked client in tests that returns canned refined text. We rely on `--no-narrative`
runs in CI to validate the fallback path.

CI runs via `.github/workflows/python-validation.yml` (new — to add alongside the
existing `sql-validation.yml`).

---

## 14. Dagster asset registration

Each routine is registered as a Dagster asset in
`orchestration/orchestration/assets/reports/<name>.py`:

```python
from dagster import asset, ScheduleDefinition, define_asset_job, RetryPolicy
from reporting.reports.recruiter_activity import RecruiterActivityReport

@asset(
    group_name="daily_reports",
    description="Daily Recruiter Activity Report — emails morning summary to recruiting leads",
    retry_policy=RetryPolicy(max_retries=2, delay=60),
    deps=["v_applicant_activity_events"],   # silver-layer asset name
)
def recruiter_activity_report(context):
    report = RecruiterActivityReport()
    result = report.run()
    context.add_output_metadata({
        "report_date": str(result.window.report_date),
        "recipient_count": result.recipient_count,
        "narrative_used": result.narrative_used,
        "duration_seconds": result.duration_seconds,
        "pdf_size_bytes": result.pdf_path.stat().st_size,
    })
    return result

# In schedules.py:
recruiter_activity_schedule = ScheduleDefinition(
    job=define_asset_job("recruiter_activity_job", selection=[recruiter_activity_report]),
    cron_schedule="0 12 * * 1-5",   # 7:00 AM ET (Droplet UTC) — Mon–Fri
    execution_timezone="America/New_York",
)
```

Dagster handles:

- **Scheduling** — replaces cron entirely.
- **Retries** — `RetryPolicy` declared per asset.
- **Failure alerting** — configured at the Dagster Cloud project level (Slack
  webhook or email distribution).
- **Lineage** — `deps=[...]` ties the report to upstream silver-layer assets so the
  Dagster UI shows "v_applicant_activity_events → recruiter_activity_report".
- **Backfills** — rerun yesterday's report by clicking a button or via
  `dagster asset materialize`.
- **Auto-deploy** — push to `main`, Dagster Cloud picks it up.

No cron, no Droplet, no `/etc/inno-reporting/env`, no `journalctl`. The Phase 3 plan
in the original prompt is replaced by **"sign up Dagster Cloud Solo, point it at the
GitHub repo, configure secrets, enable schedule"** — see `MIGRATION.md` for the
checklist.

---

## 15. DP-ID assignment

Each report gets a Data Product ID following the framework in
`REPO_RECOMMENDATIONS.md` and the registry in `metabase/catalog.yaml`. Reports are
data products even though their delivery is email rather than a Metabase dashboard —
the assessment criteria (clear purpose, defined users, ownership, documentation,
trust/reliability, measurable value, lifecycle management) all apply.

Proposed DP-IDs (subject to Sean's confirmation when the catalog is next reviewed):

| Routine | Proposed DP-ID | Notes |
|---------|----------------|-------|
| recruiter-activity | `DP-021` | Pending — assign next available ID in registry |
| position-status | `DP-022` | Pending |
| terminations | `DP-023` | Pending |

The catalog entry for a report-type data product references the
`reporting/routines/<name>.md` spec, the Dagster asset name, the source views, and
the recipient section name.

---

## CHANGE LOG

| Version | Date | Author | Notes |
|---|---|---|---|
| 0.1 | 2026-04-27 | Sean Beal | Initial framework spec — pre-implementation. Phase 1 of three reports' migration. Drafted in cowork. |
| 0.2 | 2026-04-27 | Sean Beal | Moved to data-platform repo. Replaced cron+Droplet sections (§ 11, § 14) with Dagster Cloud orchestration. Added § 15 DP-ID assignment. Updated paths throughout. Added `RunResult` data class for Dagster materialization output. |
