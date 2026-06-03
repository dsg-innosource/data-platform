---
title:        Daily Recruiter Activity — Routine Spec
routine_name: recruiter-activity
proposed_dp_id: DP-021
version:      0.3
author:       Sean Beal
status:       draft — Phase 1 spec
last_updated: 2026-04-27
extends:      reporting/routines/_framework.md
source_view:  database/views/silver/v_applicant_activity_events.sql
---

# Daily Recruiter Activity — Routine Spec

This is the **per-report** spec. It describes only what is unique to the daily
recruiter activity report; everything else (lifecycle, window logic, narrative
integration, recipient management, guards, paths, CLI, env vars, Dagster asset
registration, DP-ID assignment) lives in [`_framework.md`](./_framework.md).

This spec is the canonical, self-contained definition of the routine. The Jinja2
templates in `reporting/src/reporting/reports/recruiter_activity/templates/` are
the canonical implementation of visual layout. Together, this spec and those
templates are sufficient to reproduce or modify the report — no other reference
is required.

---

## 1. Routine identity

| Field | Value |
|-------|-------|
| `routine_name` | `recruiter-activity` |
| Proposed DP-ID | `DP-021` (pending registry update) |
| Frequency | Mon–Fri 7:00 AM ET |
| Window | Daily (Monday rule applies — Friday on Mondays) |
| Source view | `silver.v_applicant_activity_events` (DDL: `database/views/silver/v_applicant_activity_events.sql`) |
| Recipients section | `recruiter-activity-report` in `reporting/config/recipients.md` |
| Subject (Tue–Fri) | `🐾 Daily Recruiter Activity — {report_date_long}` |
| Subject (Monday) | `🐾 Daily Recruiter Activity — {report_date_long}` (date is Friday's) |
| Output base name | `activity-{YYYY-MM-DD}` (date = `report_date`, not `today`) |
| Dagster asset | `recruiter_activity_report` in `orchestration/orchestration/assets/reports/recruiter_activity.py` |
| Dagster schedule | `recruiter_activity_schedule` — cron `0 7 * * 1-5`, timezone `America/New_York` |

> **Note on file naming.** This routine uses `activity-` as the file prefix to match
> the prior production convention. The framework's default `{routine}-{date}` would
> be `recruiter-activity-…` which would break links in older Power Automate runs and
> email archives. Override the framework default in `report.py` via
> `output_basename = "activity"`.

---

## 2. Phase 1/2 testing distribution

For Phase 2 (local test) and the first Phase 3 Dagster runs, recipients are
**Sean only**:

```
sbeal@innosource.com
```

This is enforced via `recipients_override=["sbeal@innosource.com"]` on the asset
(or `--send-to sbeal@innosource.com` on the CLI), **not** by editing
`reporting/config/recipients.md`. The file keeps the full production list intact so
we can flip back with a single change. The framework appends `[DEV]` to the subject
whenever an override is in effect, so a misrouted test is obvious.

When ready for cutover, the override is removed and the full distribution list
(currently 19 addresses) takes over.

---

## 3. Source view and event categories

| View | DDL | Purpose |
|------|-----|---------|
| `silver.v_applicant_activity_events` | `database/views/silver/v_applicant_activity_events.sql` | Event-grain funnel activity log — one row per recruiter action |

Key fields used by this report:

| Field | Description |
|-------|-------------|
| `event_date` | Calendar date the action was recorded — the primary filter axis |
| `event_category` | Normalized string — see mapping table below |
| `recruiter_id` | ID of the recruiter who performed the action |
| `recruiter_name` | Display name |

`event_category` enum mapping:

| Value | Meaning | KPI / column |
|-------|---------|--------------|
| `PHONE_SCREEN` | Phone screen completed | Phone Screens / Screens column |
| `INNO_INTERVIEW` | InnoSource interview | Inno Interviews / Inno column |
| `CLIENT_INTERVIEW` | Client interview | Client Interviews |
| `OFFER` | Offer extended | Offers Extended |
| `OFFER_ACCEPTED` | Offer accepted | Offers Accepted / Accepted column |
| `HIRED` | Hired | Hires |
| `OFFER_DECLINED` | Offer declined | Excluded from all KPI counts |

`OFFER_DECLINED` events exist in the view for future use but are excluded from
all Section 02 KPI counts. The acceptance rate denominator is `OFFER` events
only — `offer_acceptance_rate = OFFER_ACCEPTED / NULLIF(OFFER, 0)`.

---

## 4. SQL

The window resolution is handled in Python by `framework.window.report_window(today)`
— no SQL needed for date math. Three queries run against the warehouse, all
parameterized by `:window_start` and `:window_end`.

### 4.1 Section 02 totals

```sql
SELECT
    COUNT(*) FILTER (WHERE event_category = 'PHONE_SCREEN')     AS phone_screens,
    COUNT(*) FILTER (WHERE event_category = 'INNO_INTERVIEW')   AS inno_interviews,
    COUNT(*) FILTER (WHERE event_category = 'CLIENT_INTERVIEW') AS client_interviews,
    COUNT(*) FILTER (WHERE event_category = 'OFFER')            AS offers_extended,
    COUNT(*) FILTER (WHERE event_category = 'OFFER_ACCEPTED')   AS offers_accepted,
    COUNT(*) FILTER (WHERE event_category = 'HIRED')            AS hires
FROM silver.v_applicant_activity_events
WHERE event_date BETWEEN :window_start AND :window_end;
```

### 4.2 Section 03 per-recruiter breakdown

```sql
SELECT
    recruiter_name,
    COUNT(*) FILTER (WHERE event_category = 'PHONE_SCREEN')   AS screens,
    COUNT(*) FILTER (WHERE event_category = 'INNO_INTERVIEW') AS inno_interviews,
    COUNT(*) FILTER (WHERE event_category = 'OFFER')          AS offers_extended,
    COUNT(*) FILTER (WHERE event_category = 'OFFER_ACCEPTED') AS offers_accepted
FROM silver.v_applicant_activity_events
WHERE event_date BETWEEN :window_start AND :window_end
  AND recruiter_id IS NOT NULL
GROUP BY recruiter_id, recruiter_name
ORDER BY offers_accepted DESC, screens DESC;
```

### 4.3 Weekend probe (Monday only)

Only runs on Monday. Counts Saturday + Sunday events to detect any weekend
activity that should be flagged in the Section 02 subtitle.

```sql
SELECT COUNT(*) AS weekend_count
FROM silver.v_applicant_activity_events
WHERE event_date BETWEEN :window_start + INTERVAL '1 day' AND :window_end;
```

When `weekend_count > 0`, the Section 02 subtitle gets `(+ N records Sat/Sun)`
appended after the date.

---

## 5. Derived metrics

Computed in `fetch()` after the queries return:

```python
oar = round(offers_accepted / max(offers_extended, 1) * 100, 1) if offers_extended else None
screen_to_inno = round(inno_interviews / max(phone_screens, 1) * 100, 1) if phone_screens else None
```

`None` semantics ripple through the callout rules and templates — `None` is the
"no offers / no screens today" path, which has its own callout rules below.

---

## 6. Threshold reference

These thresholds drive both the KPI cell coloring and the callout rule table.
They were derived from 365 days of production data (clean days only) and
validated in March 2026. **Do not change without re-running the validation
analysis.**

### Clean-day exclusions

- Screen → Inno analysis excluded days where `inno_interviews > phone_screens`
  (242 clean days from 365). These are anomalies — likely late event entry.
- OAR analysis excluded days where `offers_accepted > offers_extended` (182
  clean days from 252). These are late-entry artifacts where an acceptance was
  recorded after the offer extension was already in a prior day's window.
- Of the 252 days with any offer activity, 70 (28%) were excluded from OAR.
  OAR should not be treated as a precise daily metric — use weekly aggregates
  when possible. The daily callouts are intentionally non-alerting in the
  normal band.

### Percentile distributions

| Metric | P10 | P25 | Median | P75 | P90 | Mean |
|--------|----:|----:|-------:|----:|----:|-----:|
| Screen → Inno (242 clean days) | 21.3% | 25.6% | 31.1% | 36.7% | 45.5% | 33.4% |
| OAR (182 clean days) | 80.0% | 87.5% | 100.0% | 100.0% | 100.0% | 92.6% |

### Bands used by the report

- **Screen → Inno:** strong ≥ 37%; normal 25–36%; watch 21–24%; alert < 21%.
- **OAR:** strong ≥ 90%; normal 80–89%; watch < 80% or no offers.

---

## 7. Callout rule table

This is the deterministic decision tree that produces the structured `Callout`
objects. The framework's narrative refiner only refines wording — it cannot
change which rule fires.

### 7.1 Section 02 / Insights callouts

| `rule_id` | When | Tone | Fallback text template |
|-----------|------|------|------------------------|
| `oar-strong` | `oar >= 90` | positive | "Acceptance rate hit {oar}% on {accepted} of {extended} offers." |
| `oar-normal` | `80 <= oar < 90` | context | "Acceptance rate at {oar}% on {accepted} of {extended} offers — within normal band (80–100%)." |
| `oar-watch` | `oar < 80` | watch | "Acceptance rate at {oar}% on {accepted} of {extended} offers — below the 80% watch threshold." |
| `oar-no-offers` | `extended == 0` | context | "No offers extended today. Screen-to-inno conversion is the primary throughput signal — see below." |
| `screens-zero` | `phone_screens == 0` | watch | "Zero phone screens recorded. Funnel input is empty for the day." |
| `s2i-strong` | `screen_to_inno >= 37` | positive | "Screen-to-inno conversion at {s2i}% — above the normal 25–37% band." |
| `s2i-normal` | `25 <= screen_to_inno < 37` | context | "Screen-to-inno conversion at {s2i}% — within the normal 25–37% band." |
| `s2i-watch` | `21 <= screen_to_inno < 25` | watch | "Screen-to-inno conversion at {s2i}% — below the 25% normal floor." |
| `s2i-alert` | `screen_to_inno < 21` | alert | "Screen-to-inno conversion at {s2i}% — below the historical 21% P10. Worth investigating which screens are not advancing." |

**Severity precedence** — at most one OAR-related callout fires (`oar-watch` >
`oar-no-offers` > `oar-normal` > `oar-strong`) and at most one S2I-related callout
fires (`s2i-alert` > `s2i-watch` > `screens-zero` > `s2i-normal` > `s2i-strong`).
The Insights section thus contains **1 or 2** structured callouts on a normal day.

### 7.2 Section 03 / recruiter table callouts

| `rule_id` | When | Tone | Fallback text template |
|-----------|------|------|------------------------|
| `recruiter-zero-screens` | any recruiter has `screens == 0` | watch | "{names} recorded zero screens today. Recommend follow-up before end of day." |
| `recruiter-closer` | any recruiter has `offers_accepted > 0` | positive | "{names} closed offers today — {total} accepted across the team." |

Both can fire (independent rules). If neither fires, no Section 03 callout is
emitted.

---

## 8. Visual structure and templates

The Jinja2 templates at
`reporting/src/reporting/reports/recruiter_activity/templates/` are the canonical
implementation of visual layout. This section describes the design contract those
templates implement.

### 8.1 PDF report (`pdf.html.j2`)

A single Letter-page PDF with the following sections, in order:

| Section | Content |
|---------|---------|
| Cover top | Title bar — "Daily Recruiter Activity" + report date |
| Section 02 header | Title `[REPORT_DATE_LABEL]` + subtitle "Total recruiter throughput · [REPORT_DATE]" (with `(+ N records Sat/Sun)` on Monday when applicable) |
| Activity cards | 5 large-number cards: Phone Screens (teal), Inno Interviews (purple), Client Interviews (red), Offers Extended (amber), Offers Accepted (green) |
| Derived KPI strip | 4 cells: Offer Acceptance Rate, Screen → Inno Rate, Hires, Client Interviews. Cell class per § 8.4. |
| Section 02 callout | One callout, generated per § 7.1 rules |
| Section 03 header | Title "Recruiter Activity" + subtitle "Individual recruiter throughput · [REPORT_DATE]" |
| Recruiter table | One row per recruiter, sorted `offers_accepted DESC, screens DESC`. Columns: Recruiter, Screens, Inno, Offers, Accepted. |
| Section 03 callout | Optional — one or two callouts per § 7.2 rules |
| Footer | "InnoSource Data Governance · Daily Recruiter Activity · Source: silver.v_applicant_activity_events · Confidential — InnoSource Internal Use · Page 1" |

Base CSS comes from `reporting/styles/inno_source_report_style_guide.md`. The
template appends an "Activity Card Extension" CSS block defining `.activity-cards`,
`.activity-card`, `.activity-number`, and `.activity-label` (the large-number
cards unique to Section 02).

### 8.2 Email body (`email.html.j2`)

A mobile-responsive HTML email body. Section order:

1. **Header bar** — title + Data Governance attribution + report date
2. **Title block** — "Summary"
3. **KPI strip** (4 cells) — Screens, OAR, Screen → Inno, Hires
4. **Insights** — 1–3 callout divs (using same rules as Section 02 callout)
5. **Volume Summary** — small two-column table (label, value)
6. **Footer** — "Full report attached as PDF" + Data Governance attribution

The strip wraps to a 2×2 grid at viewport widths ≤ 600 px via an embedded
`@media` block. All overrides use `!important` to win over inline styles on
narrow screens. Apple Mail / iOS Mail / Gmail mobile respect the `<style>` block
and `@media` query; desktop clients ignore them and use the inline styles.

### 8.3 Template inputs

Both templates receive a single context dict:

```python
{
    "window": Window,                # has report_date, label_long, label_short, is_monday_run
    "kpis": {                        # Section 02 totals
        "phone_screens": int, "inno_interviews": int, "client_interviews": int,
        "offers_extended": int, "offers_accepted": int, "hires": int,
        "oar": float | None, "screen_to_inno": float | None,
    },
    "recruiters": list[dict],        # Section 03 rows, sorted
    "callouts": list[Callout],       # rendered in Insights / Section 03
    "weekend_count": int,            # Monday only
    "subject": str,                  # final subject line, including [DEV] if overridden
}
```

### 8.4 KPI cell colors (canonical)

Templates implement these tables as Jinja2 macros taking the metric value and
returning the appropriate `(bg, color)` pair.

**OAR cell:**

| Condition | Background | Text color |
|-----------|------------|------------|
| `oar >= 90` | `#F0FDF4` | `#059669` |
| `80 <= oar < 90` | `#F8FAFC` | `#0F172A` |
| `oar < 80` or `extended == 0` | `#FFFBEB` | `#B45309` |

**Screen → Inno cell:**

| Condition | Background | Text color |
|-----------|------------|------------|
| `s2i >= 37` | `#F0FDF4` | `#059669` |
| `25 <= s2i < 37` | `#F8FAFC` | `#0F172A` |
| `21 <= s2i < 25` | `#FFFBEB` | `#B45309` |
| `s2i < 21` or `phone_screens == 0` | `#FEF2F2` | `#DC2626` |

**Default cells (Screens, Hires, Client Interviews):** background `#FFFFFF`,
text color `#0F172A`.

### 8.5 Callout border / background colors

Each callout div:

```html
<div style="border-left:4px solid {border};background:{bg};border-radius:0 6px 6px 0;
            padding:12px 14px;margin-bottom:10px;font-size:13px;color:#0F172A;line-height:1.5;">
  <strong>{lead}</strong> {detail}
</div>
```

| Tone | Border | Background |
|------|--------|------------|
| positive | `#059669` | `#F0FDF4` |
| watch | `#B45309` | `#FFFBEB` |
| context | `#0E7490` | `#F0FDFA` |
| alert | `#DC2626` | `#FEF2F2` |

---

## 9. Visual invariants (style guards)

These rules are enforced as assertions in
`tests/reports/test_recruiter_activity_render.py`. After the templates render
against a fixture data set, the test parses the HTML and asserts each
invariant. Any future template edit that violates one fails CI.

| # | Rule | Correct | Wrong |
|---|------|---------|-------|
| 1 | `.dt` table headers | `background: #F1F5F9`, text `color: var(--slate)` | `background: var(--ink)` (dark headers) |
| 2 | KPI cell for attention metric | `class="kpi kdng"` | `class="kpi dng"` or `class="kpi warn"` |
| 3 | KPI cell for watch metric | `class="kpi kwarn"` | `class="kpi warn"` |
| 4 | Derived metrics KPI strip | Always rendered after activity cards | Omitted when no offers / no screens |
| 5 | Offer acceptance rate | Calculated as `OFFER_ACCEPTED / NULLIF(OFFER, 0)` | Decline events in denominator |
| 6 | Section 02 callout order | Positive finding → context → watch item | Open with a `.warn` callout |
| 7 | Recruiter table sort | `offers_accepted DESC, screens DESC` | Alphabetical |
| 8 | Page footer source | `silver.v_applicant_activity_events` | Generic "data warehouse" |

---

## 10. Local test plan (Phase 2)

Once `reporting/src/reporting/reports/recruiter_activity/` is implemented:

```bash
# 1. Window sanity — print computed window for today
python -m reporting.cli recruiter-activity --window-only

# 2. Dry run — render files, do not send. Inspect the output PDF visually.
python -m reporting.cli recruiter-activity --dry-run
open .report-out/activity-$(date +%Y-%m-%d).pdf

# 3. Send to Sean only
python -m reporting.cli recruiter-activity --send-to sbeal@innosource.com

# 4. Validate fallback path — disable narrative, send again
NARRATIVE_DISABLE=1 python -m reporting.cli recruiter-activity \
    --send-to sbeal@innosource.com

# 5. Run via Dagster locally before deploying to Dagster Cloud
cd orchestration && dagster dev
# Then in another terminal:
dagster asset materialize --select recruiter_activity_report
```

For visual fidelity verification, compare the dry-run PDF against any recent
production PDF you have on hand (a copy emailed to you in the past week works).
Acceptance criteria:

- Numbers in the new PDF match the corresponding day's prior production output.
- Email body renders correctly in Apple Mail and Outlook web.
- The `--no-narrative` run produces a working email with rule-table fallback text.
- All tests in `reporting/tests/` pass.
- `dagster dev` boots the project and `recruiter_activity_report` shows in the
  asset graph with `v_applicant_activity_events` as an upstream dep.

---

## 11. Open questions (resolve before / during Phase 2)

1. **DB network access from Dagster Cloud.** The warehouse Postgres is on Digital
   Ocean. Dagster Cloud Solo Plan supports a Hybrid agent that runs in
   customer infrastructure — that's likely what we want here so the agent can
   reach the DB on its private network. Alternative: open a public IP on the
   warehouse restricted to Dagster Cloud's egress IP range. Decision needed
   before Phase 3.
2. **Anthropic API key provisioning** — separate prod key scoped to this workload
   (recommended for cost tracking and rotation independence) or reuse Sean's
   existing key. Either way, the key goes into Dagster Cloud secrets.
3. **DP-ID assignment** — `DP-021` is proposed. Before finalizing, check what the
   highest-assigned DP-ID is in `metabase/catalog.yaml` and pick the next
   available slot. Reports may want their own DP-ID range (e.g. `DP-021`–`DP-040`
   reserved for reports) to leave Metabase models room.
4. **Power Automate webhook URL stability** — the current URL includes a
   SAS-style signature parameter (`sig=...`). Confirm with the Power Automate
   flow owner whether this URL rotates and how. Worth setting up an alert on
   401/403 from the webhook so we catch a key rotation immediately.

---

## CHANGE LOG

| Version | Date | Author | Notes |
|---|---|---|---|
| 0.1 | 2026-04-27 | Sean Beal | Initial routine spec drafted in cowork. |
| 0.2 | 2026-04-27 | Sean Beal | Moved to data-platform repo. Updated paths, added Dagster asset reference, proposed DP-ID, added Hybrid-agent question. |
| 0.3 | 2026-04-27 | Sean Beal | Made spec self-contained — inlined SQL (§ 4), KPI color tables (§ 8.4), callout colors (§ 8.5), and visual invariants (§ 9). Removed references that implied cowork was the source of truth for visual layout. The Jinja2 templates in this repo are the canonical visual implementation. |
