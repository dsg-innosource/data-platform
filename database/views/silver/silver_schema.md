# Silver View Change Log

Dated change log for the version-controlled `silver.*` view DDL in this directory.
Newest entries first. Each view file also carries its own `Modified:` header notes;
this file is the cross-view index of material, behavior-changing edits.

---

## 2026-07-14 — `v_hires_unified`: cross-req double-count fix + termination-predates-hire exclusion

**Files:** `v_hires_unified.sql`.
**Deploy:** full hires chain — `v_hire_retention` depends on this view, so redeploy order is drop `v_hire_retention` → rebuild `v_hires_unified` → rebuild `v_hire_retention` (i.e. run `deploy_hires.sql`, or the three steps manually). Both fixes flow into `v_hire_retention`'s start denominator.

### Why

Reconciling the warehouse against Portal's `rsp_rpt_next_hires_report` ("Next Hires" spreadsheet) surfaced two hire-count defects. (1) The 2026-07-01 requisition-scoped pending guard only suppresses a pending row when a Hired event exists on the **same** inferred (Accept Offer) requisition — so when someone accepts an offer on a *pipeline* req but the Hired event is logged on a *different placement* req for the same start, the pending row survived alongside the confirmed row and the same start was counted twice (885 applicants, all cross-order; e.g. Prescribe Fit applicant 1428339 on reqs 9287/9393, both 2026-07-13). (2) Portal excludes a hire whose stored `terminated_date` pre-dates its `hire_date` (a stale prior-stint termination left on the record); the warehouse had no equivalent and counted it.

### What

(1) **Cross-requisition de-dup.** `applicant_latest_event` now carries `latest_event_date`; the `pending_hires` CTE gains a second suppression guard — drop the pending row when the confirmed spine already carries this start, i.e. the applicant's latest Hired event falls on/before `hire_date + 365` (the episode window that stamps `episode_start = hire_date` on that confirmed row). Genuine new stints (>365d after the last logged event) are preserved; fully-unlogged fresh classes (no event at all) are untouched. Invariant added: no `(applicant_id, actual_start_date)` is both confirmed and pending.

(2) **Termination-predates-hire exclusion.** `canonical_users` now carries `terminated_date`; both branches adopt Portal's guard `hire_date <= terminated_date OR terminated_date IS NULL`. Applied on the current-episode start only (`es.episode_start` on confirmed — NULL on older events, so deep history is intact; `ph.hire_date` on pending).

### Validation

Read-only against prod pre-deploy and re-verified post-deploy: confirmed+pending same-start pairs 885 → **0**; termination-predates-hire rows → **0** (46 removed: 39 confirmed current-episode + 7 pending); current ISO week 29 went 21 → **19 rows**, both duplicate Prescribe Fit rows gone; `v_hire_retention` rebuilt (1,717 rows). Matches Portal's Next Hires report person-for-person on this dimension. New regression checks 2e3 / 2e4 added to `v_hires_unified.sql`.

---

## 2026-07-09 — New view: `v_active_roster` (per-associate active roster + tenure)

**Files:** `v_active_roster.sql` (new); `v_department_offering.sql` (deploy-note only — added as CASCADE step 5).
**Deploy:** standalone — run `v_active_roster.sql` (DROP IF EXISTS + CREATE). Depends on `v_department_offering`, so a future `v_department_offering` CASCADE redeploy drops it; recreate after the unified views (now step 5 in that file's sequence).

### Why

`v_hire_retention` was being used to answer "active associate tenure," but it's a retention-CHECKPOINT surface capped to hire starts in the trailing 365 days, so it excludes long-tenured actives (Goodyear InfoLink: it showed 8 of 40 active; the 32 tenured >1yr were dropped by the cap). `fact_active_headcount` is aggregate counts only; `v_oso_roster_unified` is OSO-only. No surface answered "who is active now and how long have they been here" across all clients.

### What

New `silver.v_active_roster`: one row per active ADP position (`position_id` unique) as of the latest `bronze.adp_tenure_history` snapshot (`position_status='Active'`). ADP-native — ~18% of active rows have NULL `applicant_id`, so name/dept/client come from ADP and `applicant_id` is a nullable link to `v_hires_unified`/`v_terminations_unified`. Client resolves via `v_department_offering` (matches hires/terminations) with raw `adp_client`/`adp_client_code` as a completeness fallback for unmapped departments. Tenure bands match siblings (Short<90 / Mid<365 / Long≥365). Excludes `regular_pay_rate` and `home_phone` (PII).

### Validation

Live: 2,082 active positions / 1,715 distinct applicants as of the latest snapshot; 246 rows have an unmapped department (reporting_client NULL — surfaces the gap). Goodyear InfoLink reproduces 40 active (8 started <365d = what `v_hire_retention` shows; 32 tenured ≥1yr excluded by its cap), reconciling the Minnie discrepancy — and correctly counting the null-`applicant_id` actives an applicant-keyed query drops.

---

## 2026-07-08 — Add `reporting_client` client-family rollup (all four views)

**Files:** `v_department_offering.sql` (source column), `v_terminations_unified.sql`,
`v_hires_unified.sql`, `v_hire_retention.sql` (surface it).
**Deploy:** changing `v_department_offering` means a full-chain rebuild — its
`DROP … CASCADE` drops all three dependents; redeploy order:
`v_department_offering` → `v_hires_unified` → `v_terminations_unified` → `v_hire_retention`.

### Why

`client_name` is the **granular portal client entity**, and those entities come in
**families**: "Univar" is four separate portal clients (Univar Solutions, Monument
Consulting Univar, …AZ, …CA — plus NEXEO/SUPERIOR rolls up to Univar too). A
client-level question ("how is Univar doing?") that matches a single `client_name`
literal undercounts, and a fuzzy `ILIKE '%univar%'` misses oddly-named members
(NEXEO/SUPERIOR has no "Univar" in the name).

`silver.department_reporting_map` already carries the curated rollup in its `client`
column (all Univar departments → `Univar Solutions`), but `v_department_offering`
was dropping it — pulling only `offering`/`service` from the map. So the rollup
existed but never reached the views.

### Change

- `v_department_offering`: expose `department_reporting_map.client` as a new
  `reporting_client` column (both UNION streams + final SELECT). Same join and
  grain as `offering`/`service` — no fan-out; NULL for codes absent from the map.
- `v_terminations_unified`, `v_hires_unified` (both branches): select
  `vdo.reporting_client` (resolved on the same point-in-time `department_code`).
- `v_hire_retention`: pass `h.reporting_client` through the `hires` CTE (`SELECT hr.*`
  surfaces it automatically).

### Client model — which field to use (reference)

| Field | Meaning | Use for |
|---|---|---|
| `client_id` / `client_name` | Granular **portal** client entity (made point-in-time correct in the fix below) | Operational, entity-level queries; joins to orders/positions |
| `reporting_client` | Curated client-**family** rollup from `department_reporting_map.client` | Client-level reporting / "how is *<client>* doing" — resolves the whole family |

**Guidance for report/question interpretation:** resolve a named client (Univar,
Nationwide, Goodyear, …) against **`reporting_client`**, not `client_name`. E.g.
`WHERE reporting_client = 'Univar Solutions'` returns the full family. Fall back to
`client_name` only when a specific operational entity is genuinely intended.

### Validation

Modified `v_department_offering` body run live: `reporting_client` present, 1 row per
code (no fan-out), Univar family → single `Univar Solutions`. End-to-end via the
consuming views' `department_code`: fixtures resolve correctly (Abdullahi/Broderick →
Univar Solutions, Galool/Wade → Nationwide, Dianna Hawkins → Nexeo Plastics).

---

## 2026-07-08 — Client attribution fix (hires / terminations / retention)

**Files:** `v_terminations_unified.sql`, `v_hires_unified.sql`
(`v_hire_retention.sql` unchanged — inherits client from `v_hires_unified`).
**Deploy:** `deploy_hires.sql` (rebuilds `v_hires_unified` then `v_hire_retention`);
`v_terminations_unified.sql` deploys standalone.
**Fixtures:** applicant_id 1299933 (Mohamed Abdullahi, Nationwide → Univar),
1436 (Broderick Johnson, EASE Logistics → Univar), 162331 (Galool Siad,
Univar → Nationwide), 486356 (Dianna Hawkins, Univar → Nexeo Plastics).

### Root cause

`client_id` / `client_name` were resolved from **application-era** data, not from
where the associate actually worked at the event:

- `v_terminations_unified` led with `COALESCE(a.client_id, r.client_id,
  udf.dept_client_id)` — `portal_applicants.client_id` is a single mutable row
  (a stale value from an earlier application, e.g. a 2015 client, overrides
  everything), and the AJL-resolved requisition can be a **pipeline** req whose
  client is a bench label (e.g. a "Nationwide … Pipeline" req attached to a
  Univar associate).
- `v_hires_unified` (both branches) led with `COALESCE(r.client_id, a.client_id)`
  — same pipeline-req exposure.
- Both views already resolve the **point-in-time department** (ADP snapshot at
  the event, then onboarding dept) and join `silver.v_department_offering`, which
  carries a matching `client_id` / `client_name` — but that client was **discarded**,
  used only for the `department_*` columns. So the client and department columns
  of the same row could disagree (client "Nationwide", department "Univar …").

This resolves the client-name residual limitation the 2026-07-01 entry explicitly
left "as-is" (position_title is still applicant-mutable and remains open).

### Changes

1. **`v_terminations_unified`:** `client_id = COALESCE(vdo.client_id, a.client_id,
   `*non-pipeline*`r.client_id, udf.dept_client_id)`; `client_name =
   COALESCE(vdo.client_name, c.name, udf.dept_client_name)`.
2. **`v_hires_unified` (confirmed + pending branches):** `client_id =
   COALESCE(vdo.client_id, `*non-pipeline*`r.client_id, a.client_id /
   ph.applicant_client_id)`; `client_name = COALESCE(vdo.client_name, c.name)`.
3. **`portal_clients` join (`c`) made pipeline-aware** in every branch so `c.id`
   equals the applicant/requisition tiers of the `client_id` COALESCE — `client_id`
   and `client_name` therefore always resolve from the **same** tier (`vdo`, `c`,
   and `udf` each supply a matched id/name pair).
4. **`v_hire_retention`:** no edit — it selects `client_id` / `client_name` straight
   from `v_hires_unified` and is corrected automatically.

`vdo.client_id` (ADP = payroll system of record) is deterministic: `v_department_
offering` dedupes to one client per department code (`DISTINCT ON (code)`, prefer
active/highest-id), so no row fan-out.

### Blast radius (measured, before → after)

| Metric | Before | After |
|---|---|---|
| `v_terminations_unified` rows | 26,605 | 26,605 (unchanged) |
| Terminations client re-attributed | — | **5,519** (~21%) |
| `v_hires_unified` rows | 23,550 | 23,550 (unchanged) |
| Hires client re-attributed | — | **3,319** (~14%) |
| Rows newly NULL client (either view) | — | **0** |
| Rows with `client_id` set but `client_name` NULL | 0 | **0** |
| H1-2026 Univar terminations (client_id ∈ {567,577,581,591}) | 57 | **55** (+2 / −4) |
| H1-2026 Univar hires | 42 | **41** |

Re-attribution verified against ADP as corrections in **both** directions
(H1-2026 Univar: +Abdullahi/+Broderick, −Wade Lewis/−Galool Siad/−Dianna Hawkins×2).

### ⚠ Downstream consideration — client families

The fix makes `client_name` more granular/accurate. Clients exist in **families**:
Univar = `client_id {567, 577, 581, 591}` (Solutions / Monument Consulting / AZ / CA);
Nationwide splits into PA and S&S variants. **Consumers filtering `client_name` on a
single literal must switch to a `client_id` set** or they will undercount after deploy.

### Residual limitations (documented, not fixed)

- `position_title` may still reflect the applicant's most-recent placement
  (`portal_applicants` is a single mutable row). Only client was corrected here.
- ~4,500 pre-2025 changes are `onboarding_dept`-sourced (no ADP snapshot to confirm);
  they rely on the onboarding department beating a possibly-stale applicant record —
  defensible (matches the Broderick fixture) but not independently verifiable.

### Validation

Modified view bodies were executed live (SELECT-only, `dwreader`) and diffed against
the current prod views: row counts unchanged, 0 new NULLs, 0 id-without-name,
change counts and the four fixtures as above.

---

## 2026-07-01 — Episode-mismatch enrichment fix (hires / terminations / retention)

**Files:** `v_hires_unified.sql`, `v_terminations_unified.sql`, `v_hire_retention.sql`
**Deploy:** `deploy_hires.sql` (rebuilds `v_hires_unified` then `v_hire_retention`);
`v_terminations_unified.sql` deploys standalone.
**Fixture:** applicant_id 136818 (Jacqueline Adamson) — 2017 Nationwide Iowa hire
(order 1331, OSO), re-applied 2026 to Alliant Energy CSR, cancelled "Never started /
delayed" 2026-05-04.

### Root cause

Person-level fields were decorated by `applicant_id` **alone** from single, mutable
master records, not bound to the hire **episode**:

- `bronze.portal_users` holds **one** row per applicant (collapsed by the
  `canonical_users` CTE). Its `hire_date` and `onboarding_department_id` are
  overwritten on rehire — they reflect the **current** stint. These drove
  `actual_start_date` / department on the confirmed hire spine, and `hire_date` /
  `tenure_days` on terminations. A later re-application's start/department was
  therefore pasted onto an **older** event/termination row.
- The confirmed ADP-PIT department lookup (`snapshot_date >= event_date`, earliest)
  had no upper bound, so for an old event with only later ADP snapshots it backfilled
  a **later** episode's department.
- The pending anti-double-count guard (`NOT EXISTS any type-6 event for the
  applicant`) suppressed a genuinely **new** episode for anyone ever confirmed-hired,
  so rehires were invisible.
- `v_hire_retention.term_match` matched only `is_excluded = false` terminations, so an
  **EXCLUDED** "Never started / delayed" cancellation did not deactivate the row — a
  no-show read as active and retained.

### Changes

1. **`v_hires_unified` (confirmed spine):** `actual_start_date` / `start_iso_*` /
   `days_to_start` are bound to the correct episode via a new `applicant_latest_event`
   anchor + `es` LATERAL — the stored start decorates **only** the applicant's most-
   recent Hired event, and only when it lands on/before `event_date + 365` (no lower
   bound: a start before the event is normal event-logging lag). Older/rehire and
   far-later starts → NULL. ADP-PIT lookup gains upper bound `snapshot_date <=
   event_date + 365`; onboarding-dept fallback gated to the current episode.
2. **`v_hires_unified` (pending source):** anti-double-count guard is now
   **requisition-scoped** (same inferred order only; applicant-wide fallback when no
   Accept Offer anchors an episode) so new-stint rehires surface with their own
   order/start. Evidence-gate **arm 2** now ignores `termination_reason_id = 13`
   (never-started) terminations. **Invariant change:** confirmed/pending exclusivity
   is now per `(applicant_id, order_id)`, not per applicant.
3. **`v_terminations_unified`:** `hire_date` / `tenure_days` / `tenure_band` treat a
   stored start that **postdates** the termination as unmatched → NULL. `tenure_days`
   can no longer be negative.
4. **`v_hire_retention`:** starts cancelled as never-started (an EXCLUDED termination
   on/after the start) are removed from the surface entirely — a no-show belongs in
   neither the retention numerator nor the denominator.

### Blast radius (measured, before → after)

| Metric | Before | After |
|---|---|---|
| Confirmed rows with `actual_start_date - event_date > 365d` | 117 (108 applicants) | **0** |
| Retention rows whose confirmed event predates start by `>180d` | 46 (42 appl; 24 active, 34 retained_6w) | **10** (latest-event, ≤340d; genuine long lag) |
| Retention `currently_active` with EXCLUDED term on/after start | 136 | **0** |
| Terminations with `tenure_days < 0` | 602 (264 non-excluded; worst −664,635) | **0** (602 nulled as unmatched) |
| New-episode rehires surfaced in pending | 0 (suppressed) | **903** (0 same-order double-counts) |

### Residual limitations (documented, not fixed)

- An older stint's true start is not reconstructed (overwritten in `portal_users`);
  NULL is the honest value. Future: recover from `bronze.adp_tenure_history.hire_date`
  where ADP covers the stint.
- Terminations `client_name` / `position_title` may still reflect the applicant's
  most-recent placement (`portal_applicants` is a single mutable row). No acceptance
  metric; left as-is.
- A same-order rehire (new stint on the *same* requisition, no new type-6 event) is
  still suppressed by the requisition-scoped guard.

### Validation

Per-metric before/after SQL lives in the "VALIDATION QUERIES" block of each view file
(`v_hires_unified.sql` 2e/2e2, `v_terminations_unified.sql` 11/12) plus the acceptance
queries in the PR/handoff notes. All four metrics confirmed against live silver
(SELECT-only) using applicant_id 136818 as the fixture.
