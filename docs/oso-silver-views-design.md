# OSO Executive Reporting — Silver View Design

**Status:** Draft for review — v1 scope agreed 2026-06-04
**Owner:** Sean Beal
**Target:** Minnie Studio OSO dashboard (Executive section), per mockup

## Purpose

A unified silver layer over the per-client OSO bronze tables (`alliant_*`, `aep_*`)
to power the OSO executive dashboard: per-client SLA cards (ASA, AHT, abandon %,
weekly hours) plus program-level headcount, total hours, and net gain.

## Source-validation findings (2026-06-04, ad-hoc with Jourdan — post-deploy)

A review of agreed/client-facing sources changed the v1 scope after the views
were deployed:

| Finding | Impact |
|---|---|
| **AEP ASA/AHT/abandon confirmed correct.** The agreed source is the half-hour reports; `bronze.aep_oso_daily_data` (used by `v_oso_call_metrics_daily`) is a bronze VIEW that rolls up `bronze.aep_oso_half_hour_data` — verified identical to the decimal. | No change. Remaining work is ingest only: the half-hour feed is stale (max 2026-05-14) and has historical gaps. |
| **Hours (both clients) — wrong basis; now its own project.** Authoritative "Hours (Week)" = ADP payroll-processed/invoiced hours, currently living in a SharePoint sheet (Abby → Steve). Our attendance/login hours don't match (Alliant ~6,300/wk vs packet ~3,180). Investigated `bronze.portal_invoice*` as a source: the schema is ideal (per-associate hours by category, weekly periods) and the feed is current, but **AEP OSO depts have zero invoices ever and Alliant's last is 2025-02** — both clients' billing left the portal during 2025 (checked alternate client names, NULL depts, ADP company codes, Tungsten files: nothing). Open question for Abby/accounting: what system generates AEP/Alliant invoices since mid-2025, and can we feed from it? | Hours deferred dashboard-wide. `v_oso_hours_daily` re-documented as operational attendance/login time, NOT the exec hours basis. Weekly-summary hours columns marked provisional. |
| **Alliant AHT — wrong source.** Agreed source is the client "intraday report" (no system access; manual screenshot → spreadsheet → Power BI today). Our NICE `daily_aht`-derived value (~480s) is real platform data but won't match client-facing numbers. Plan: ingest the intraday spreadsheet as `bronze.alliant_energy_intraday` (would supply ASA + AHT). | Alliant AHT suppressed in the dashboard payload until the intraday feed exists. View still computes it (documented as platform-derived). |
| **Alliant abandon — confirmed not available** ("I don't think we get that"). | Already NULL; deferral was correct. |
| **Alignment note.** Jourdan is building AEP client/Tony-facing views in Metabase. The silver layer and his models must agree but won't be identical; reconcile when his SQL lands. | Follow-up. |

Dashboard scope after findings: program headcount + net gain (ADP), AEP card
with ASA/AHT/abandon only, Alliant card pending, in-progress banner.
See `minnie-studio/TASK_OSO_DASHBOARD_CHANGES.md`.

## Scope decisions (2026-06-04)

| Decision | Choice |
|---|---|
| Alliant ASA / abandon / SLA compliance | **Deferred.** Only source is `bronze.alliant_energy_repeat_callers`, which stopped loading 2025-07-31. Alliant v1 ships AHT + hours + headcount. Views carry the columns as NULL so the shape doesn't change when the feed returns. |
| Hours (Week) definition | ~~Paid/worked hours from attendance/login~~ **SUPERSEDED 2026-06-04 (see findings above):** authoritative hours = ADP payroll/invoiced hours; feed TBD — its own project. Attendance/login hours retained in `v_oso_hours_daily` as operational data only. |
| Targets (ASA 75s/90s, AHT 300s/530s, abandon 7.5%/5%, hours) | **Deferred** until canonical storage location is found. Views expose raw metrics only; GREEN/YELLOW status computed once targets land (planned: `silver.oso_sla_targets` seed table with effective dating). |
| Headcount / net gain | **Contract Staffing methodology, no new view.** Minnie Studio's headcount report queries `silver.fact_active_headcount` JOIN `silver.department_reporting_map` directly (no view); the OSO report reuses the same queries with `WHERE drm.offering = 'OSO'` and `GROUP BY drm.client`. Monday snapshots; net gain = WoW delta. Covers all OSO clients (AEP, Alliant, FedEx, Goodyear, ADS, SVdP). |
| Goodyear section of mockup | Out of scope — no data yet. |
| Revenue / margin | Pending financial integration (mockup shows "Pending"). |

## Source inventory

### AEP (`bronze.aep_*`)

| Table | Grain | Used for | Freshness (6/4) |
|---|---|---|---|
| `aep_oso_daily_data` | program/day | ASA (`total_answer_speed`), AHT (`total_handle_time / total_accepted_calls`), abandon (`abandoned / total_calls`), volume | thru 2026-05-14 |
| `aep_oso_half_hour_data` | program/half-hour | intraday drill-down (later) | thru 2026-05-14 |
| `aep_oso_agent_stats` | agent/day | agent AHT, call counts, off-phone (agent drill-down later) | thru 2026-05-29 |
| `aep_oso_login` | agent/session | paid/worked hours (session durations) | thru 2026-05-18 |
| `aep_oso_supervisor` | agent | roster: supervisor, location, class, active flag | current-state |
| `aep_oso_target_calls` | day | call-volume target + cost/day (not SLA targets) | thru 2026-12-31 |

⚠ AEP loads lag 1–3 weeks. Verify Airbyte/ingest cadence before dashboard launch.

### Alliant (`bronze.alliant_energy_*`)

| Table | Grain | Used for | Freshness (6/4) |
|---|---|---|---|
| `alliant_energy_daily_aht` | agent/day | AHT (`"Handle Time"` weighted by `Handled`), inbound volume | thru 2026-06-01 |
| `alliant_energy_attendance` | agent/activity/day | paid/worked hours (`paid_hours`, activity codes) | thru 2026-06-01 |
| `alliant_energy_team_roster` | agent | roster: status, team lead, hire date, Agent ID ↔ Employee # | current-state |
| `alliant_energy_repeat_callers` | contact | ASA (`InQueue`), abandon (`Abandon` Y/N), SLA flag — **stale, deferred** | ended 2025-07-31 |
| `alliant_energy_agent_state` | agent/state interval | phone-state drill-down (later) | thru 2026-05-30 |

Other alliant tables (quality_scores, digital_messages, ecomail, surveys, discipline,
my_account_*) feed the agent scorecard, not the exec rollup. The Metabase
"Scorecard Models" collection holds the agent-level monthly logic if/when we add
an agent drill-down. Metabase's "Total Contacts" model (calls + digital + ecomail)
is the precedent if exec reporting ever needs contacts instead of calls.

### Headcount

- `silver.fact_active_headcount` (Monday snapshots, by department_number)
- `silver.department_reporting_map` — OSO mapping: AEP (112996, 112998),
  Alliant Energy (110307, 110308), plus FedEx / Goodyear OSO / ADS OSO / SVdP.

## Proposed views (v1)

All in `database/views/silver/`, following the `v_*_unified` conventions
(header comment block, `source_system` column, explicit casts).

### 1. `silver.v_oso_call_metrics_daily`

One row per client per day. The UNION seam for telephony stats.

| Column | AEP source | Alliant source |
|---|---|---|
| `client_name` ('AEP' / 'Alliant Energy') | literal | literal |
| `metric_date` | `"Date"` | `"Login Date"` |
| `calls_offered` | `total_calls` | NULL (deferred) |
| `calls_handled` | `total_accepted_calls` | SUM(`Handled`) |
| `calls_abandoned` | `abandoned` | NULL |
| `abandon_rate` | abandoned / NULLIF(total_calls,0) | NULL |
| `asa_seconds` | `total_answer_speed` | NULL |
| `aht_seconds` | total_handle_time / NULLIF(total_accepted_calls,0) | SUM(`"Handle Time"`) / NULLIF(SUM(`Handled`),0) |

Note: confirm `aep_oso_daily_data.total_answer_speed` semantics (avg vs total —
if total, divide by accepted) and `total_handle_time` units during build, against
the Metabase AEP AHT gauge (SLA < 300s).

### 2. `silver.v_oso_hours_daily`

One row per client per agent per day: paid/worked hours.

- **Alliant:** `alliant_energy_attendance` — SUM(`paid_hours`) grouped by
  agent/date, with `is_worked` flag derived from activity code (worked codes:
  Open, Overtime, Training–*, Project–*, Meeting, Mentoring, etc.; non-worked:
  Unplanned absence, PTO, Holiday, Lunch). Exact code list to be confirmed with
  ops during build — view should carry `activity_code` through or expose the flag
  so the rule is auditable.
- **AEP:** `aep_oso_login` — SUM(end_timestamp − start_timestamp) per agent/day.
  Guard against overlapping/duplicate sessions and NULL end_timestamps.

Columns: `client_name`, `agent_name`, `work_date`, `hours`, `is_worked`, `source_system`.

### 3. `silver.v_oso_roster_unified`

One row per agent: `client_name`, `agent_name`, `agent_id` (NICE / s_number),
`employee_number`, `status` (Active/Inactive), `team_lead` / `supervisor`,
`location`, `hire_date`, `role`. Alliant from `team_roster`; AEP from
`aep_oso_supervisor`. Used for joins/drill-downs, NOT for headcount KPIs
(ADP is authoritative there).

### Headcount — no new view

The Headcount dashboard (`minnie-studio/backend/app/reports/headcount.py`)
queries `silver.fact_active_headcount JOIN silver.department_reporting_map`
directly with parameterized SQL (FILTER on snapshot dates for WoW/YoY).
The OSO report reuses the same query shapes with `WHERE drm.offering = 'OSO'`
added and `GROUP BY drm.client` for per-client cards. Program totals (OSO
headcount 481, net gain −8) fall out of the same query unsliced.

### 4. `silver.v_oso_weekly_summary`

The dashboard's main feed. Client × ISO week rollup of views 1 + 2:

`client_name`, `iso_year`, `iso_week`, `week_start`, `calls_offered`,
`calls_handled`, `calls_abandoned`, `abandon_rate`, `asa_seconds` (volume-weighted),
`aht_seconds` (volume-weighted), `total_hours` (worked only).

Targets/status intentionally absent until the targets question is resolved;
Minnie Studio computes display-only deltas meanwhile.

## Minnie Studio integration (after views ship)

- `backend/app/reports/team_oso.py` — fetch module reading
  `v_oso_weekly_summary` + the headcount queries (offering-filtered, pattern:
  `headcount.py` / `team_pipeline.py`)
- Route `GET /api/reports/team-oso` with TTL cache + prewarmer
- Page under a new sidebar section or Operations; per-client SLA cards mirroring
  the mockup; GREEN/YELLOW pills deferred with targets

## Hours addendum (2026-07-20) — ADP payroll feed landed

The "own project" from finding #2 resolved: ADP payroll hours arrived as
`bronze.assoc_hours_by_pay_period` (per-associate per pay period; **manual load
for now**, no scheduled ingest). Profile (24,773 rows, no NULLs, unique on
applicant × dept × period × pay date):

| Dept | Client | Cadence | Coverage |
|---|---|---|---|
| 112998 | AEP | weekly, Mon–Sun (= ISO weeks) | 2023-02-20 → current |
| 112996 | AEP (legacy) | bi-weekly | Jan–Feb 2023 only |
| 110307 / 110308 | Alliant Energy | bi-weekly, two cycles offset one day, **shared pay dates** | Dec 2024 → current |

Decisions (Sean, 2026-07-20):

| Decision | Choice |
|---|---|
| Grain | **Native pay-period** — actual paid hours, no weekly allocation/approximation. Alliant displays bi-weekly; AEP's pay week happens to equal the ISO week. |
| Hours basis | **Total paid = regular + OT + other** (other = paid absence; spikes on holiday weeks). |
| Program-level hours KPI | **Dropped** — weekly vs bi-weekly spans aren't comparable; hours live on the client cards only. |
| Views | `v_associate_hours_by_pay_period` (cleaned associate grain, general-purpose) → `v_oso_payroll_hours_by_period` (client × pay date; Alliant's two cycles merge on the shared pay date). Deploy in that order. `v_oso_weekly_summary` untouched; its attendance-hours columns stay provisional/suppressed. |
| Reconciliation | **Gate before dashboard ships:** feed shows Alliant ~14k paid hrs / bi-weekly period (~7k/wk-equiv) vs the packet's ~3,180/wk quoted in June — verify one known packet week (Abby/Steve SharePoint) before merge. |

Correction rows (negative hours components, 15 rows) are payroll reversals —
kept in the views; sum, don't filter.

## Open items

1. ~~Headcount source~~ → resolved: fact_active_headcount × drm.
2. Targets storage — Sean locating canonical source; then add `silver.oso_sla_targets`.
3. ~~Alliant contact-detail feed~~ → superseded: Alliant ASA/AHT will come from the intraday-report feed (`bronze.alliant_energy_intraday`, to be built); abandon confirmed unavailable.
4. **AEP half-hour ingest** — stale since 2026-05-14 with historical gaps; the only remaining blocker for AEP telephony.
5. ~~AEP semantics~~ → validated: daily_data ≡ half-hour rollup, exact match.
6. ~~Alliant activity codes~~ → moot for the dashboard (hours deferred); still relevant if v_oso_hours_daily gets operational use.
7. "SLA Compliance" KPI — needs targets + compliance rule; deferred with targets.
8. ~~Hours feed~~ → resolved 2026-07-20: ADP payroll feed (`bronze.assoc_hours_by_pay_period`) + the two views above (see Hours addendum). Remaining: reconcile vs exec packet; automate the currently-manual bronze load.
9. **Alliant intraday ingest (NEW)** — get Jourdan's spreadsheet flow into bronze; supplies Alliant ASA + AHT.
10. **Metabase alignment (NEW)** — reconcile with Jourdan's AEP Metabase models when built; agree-but-not-identical.
