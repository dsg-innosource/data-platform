# Silver View Change Log

Dated change log for the version-controlled `silver.*` view DDL in this directory.
Newest entries first. Each view file also carries its own `Modified:` header notes;
this file is the cross-view index of material, behavior-changing edits.

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
