-- ============================================================================
-- Silver Layer View: v_terminations_unified
-- Purpose: Present Portal 1.0 termination records in Portal 2.0 structure
-- Created:  2026-03-13
-- Modified: 2026-07-08 — CLIENT ATTRIBUTION FIX. client_id/client_name were
--           resolved from application-era data — portal_applicants.client_id,
--           then a possibly-PIPELINE requisition's client (via AJL), then the
--           onboarding department. A stale applicant record (e.g. a 2015 client
--           that was never updated on reassignment) or a Nationwide pipeline
--           requisition therefore overrode the client the associate ACTUALLY
--           worked for at termination. The offering enrichment below already
--           resolves the point-in-time department (ADP snapshot on/before the
--           termination, then onboarding dept) and joins silver.v_department_
--           offering, which carries a MATCHING client_id/client_name — that was
--           being discarded. Fix: prefer vdo.client_id/vdo.client_name (ADP is
--           the payroll system of record), falling back to the prior applicant→
--           requisition→onboarding chain, with is_pipeline requisitions dropped
--           from that fallback. Impact (26,605 rows): 0 newly NULL; ~21% re-
--           attribute, verified against ADP as corrections in BOTH directions
--           (H1-2026 Univar 57→55: +Abdullahi/+Broderick, −Wade Lewis/−Galool
--           Siad/−Dianna Hawkins×2). Closes the client concern the 2026-07-01
--           note below explicitly left "as-is". ⚠ Clients have FAMILIES — Univar
--           = client_id {567,577,581,591}; Nationwide has PA / S&S variants — so
--           consumers filtering client_name on a single literal must switch to a
--           client_id set or they will undercount after this change.
-- Modified: 2026-07-01 — EPISODE-BINDING FIX for tenure (defect 1). hire_date and
--           tenure_days were decorated from canonical_users.hire_date — ONE
--           mutable value per applicant (the current stint, overwritten on
--           rehire) — joined by applicant_id ALONE. When that stored start
--           POSTDATED the termination_date it belonged to a LATER episode, and
--           (termination_date - hire_date) went negative (602 rows, 264 non-
--           excluded, worst -664,635; applicant 136818's 2017 exit read
--           tenure_days -3310 off a 2026 start). Fix: treat a stored start that
--           postdates the termination as UNMATCHED — hire_date and tenure_days
--           are NULL (and tenure_band 'Unknown'). tenure_days can no longer be
--           negative. RESIDUAL: an older stint's true start is not reconstructed
--           (the value was overwritten); and client/position may still reflect
--           the applicant's most-recent placement (portal_applicants is a single
--           mutable row) — that is a separate, unmeasured enrichment concern and
--           is left as-is. See v_hires_unified 2026-07-01 for the matching fix on
--           the hire spine.
-- Modified: 2026-03-13 — Added termination_created_at, entry_iso_year,
--           entry_iso_week to support "entered this week" reporting.
--           Reports should filter on entry_iso_year / entry_iso_week
--           (record entry date) rather than iso_year / iso_week
--           (official termination date), because terminations are frequently
--           entered days or weeks after the event occurred.
--         : 2026-03-17 — Added AJL fallback for client/requisition resolution.
--           portal_applicants.client_id is NULL for ~97.6% of applicant rows;
--           the view previously left client_name NULL for those records.
--           Fix: when portal_applicants.client_id IS NULL, fall back to the
--           most recent portal_applicant_job_listings row to resolve
--           requisition_id and client_id.
--           Data analysis (2026-03-17, 21,946 non-excluded terminations):
--             70.5% resolved via portal_applicants.client_id (primary)
--             14.7% recoverable via AJL fallback
--             14.9% true orphans (neither source has client data)
--           Where both sources have data, portal_applicants.client_id is
--           authoritative (85% agree; 15% disagree due to multi-client
--           applicant history — applicants who applied at one client but
--           were placed at another).
--         : 2026-05-05 — Added user-department fallback (3rd-priority client
--           resolver). For terminations whose applicant chain dead-ends
--           (no a.client_id, no a.requisition_id, no AJL row), resolve via
--           portal_users.onboarding_department_id → portal_departments.client_id.
--           Recon (last 120 days, is_excluded = false):
--             540 terminations resolve via the prior chain
--              22 rescued by this fallback
--               0 unfixable
--           Rescued records still have NULL position_id, position_key,
--           position_title, is_pipeline, and recruiter_name — there is no
--           requisition by design. Tracked as a known limitation.
--         : 2026-06-03 — Added offering enrichment (department_code, offering,
--           service, is_offering_excluded, offering_source). Resolves the
--           department-at-termination via a LATERAL lookup into
--           bronze.adp_tenure_history (latest snapshot on/before
--           termination_date); falls back to onboarding_department_id (already
--           pulled by user_department_fallback CTE). Joined to
--           silver.v_department_offering for the offering label.
--           NOTE: bronze.portal_associates was considered and rejected as a
--           source for current placement — its `id` column overlaps with
--           portal_users.id only by numeric coincidence (~32% of associate
--           rows reference different humans from different time periods, e.g.,
--           an associate record created in 2021 colliding with a user created
--           in 2026). Use adp_tenure_history.applicant_id as the linkage.
--         : 2026-06-03 — Added INCLUDE (requisition_id) to idx_ajl_applicant_id
--           so the ajl_fallback CTE can run as an index-only scan (no heap
--           fetches). Migration on an existing deployment:
--             CREATE INDEX CONCURRENTLY idx_ajl_applicant_id_v2
--               ON bronze.portal_applicant_job_listings (applicant_id, created_at DESC)
--               INCLUDE (requisition_id);
--             DROP INDEX CONCURRENTLY bronze.idx_ajl_applicant_id;
--             ALTER INDEX bronze.idx_ajl_applicant_id_v2 RENAME TO idx_ajl_applicant_id;
--           Fresh deploys (no existing index) just run the Step 0 statement.
--
-- ⚠ PREREQUISITES — run these indexes before deploying the view:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ajl_applicant_id
--     ON bronze.portal_applicant_job_listings (applicant_id, created_at DESC);
--   Without this index, the ajl_fallback CTE will seq-scan a 1 GB table.
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_adp_tenure_applicant_snap
--     ON bronze.adp_tenure_history (applicant_id, snapshot_date);
--   Required by the offering-enrichment LATERAL (1.16M rows would otherwise
--   seq-scan per termination row). Shared with v_hires_unified — first deploy
--   of either view creates it; subsequent deploys are no-ops via IF NOT EXISTS.
-- ============================================================================
-- DESIGN NOTES:
--   Grain      : One row per termination event, AFTER collapsing duplicate
--                same-day termination records for the same person — see the
--                terminations_deduped CTE.
--   Vocabulary : Cajetan (Portal 2) field naming throughout.
--                Portal 1 source fields aliased to match.
--
--   In Portal 1: requisition = position = order (1:1:1)
--                position_id and order_id are the same value (r.id)
--                sourced from portal_applicants.requisition_id (primary)
--                or portal_applicant_job_listings.requisition_id (fallback)
--   In Portal 2: positions and orders are separate (1:many)
--                terminations → users → assignments → orders → positions
--
--   Client resolution priority (portal_v1 branch) — revised 2026-07-08:
--     1. v_department_offering.client_id      — client of the point-in-time
--                                               department (ADP snapshot at
--                                               termination, then onboarding);
--                                               ADP = payroll system of record
--     2. portal_applicants.client_id          — application-era fallback
--     3. portal_requisitions.client_id        — via AJL; PIPELINE reqs excluded
--     4. portal_departments.client_id         — via onboarding_department_id
--     5. NULL                                 — true orphan; no placement data
--
--   Source (now)    : bronze.portal_terminations  (source_system = 'portal_v1')
--   Source (future) : cajetan.terminations        (source_system = 'cajetan')
--                     Add as UNION ALL branch when Cajetan is in warehouse.
--                     Column list must exactly match the portal_v1 branch.
--
--   Date scope : NONE — callers filter by termination_date or
--                termination_created_at, and the corresponding iso week fields.
--
--   TWO DATE AXES — understand which to use:
--
--     termination_date / iso_year / iso_week
--       The official last day of work. Use for tenure calculations,
--       client-facing attrition counts, and historical trend analysis
--       where you want records anchored to the event date.
--
--     termination_created_at / entry_iso_year / entry_iso_week
--       The date/time the record was entered in the portal. Use for
--       operational "what was entered this week" reporting and recruiter
--       activity tracking. Terminations are frequently entered late
--       (typical lag: 1–5 days; max observed: 80+ days), so this axis
--       reflects current workload more accurately than termination_date.
--
--   Exclusions : is_excluded is exposed but NOT filtered here.
--                Callers should apply: WHERE is_excluded = FALSE
--                "Never started / delayed" (termination_reason_id = 13) = 4,045 rows
--
--   is_pipeline: Exposed but not filtered. Pipeline reqs have no real openings
--                and may need to be excluded from some reports. Decision pending.
--                Callers can apply: WHERE is_pipeline = FALSE
--
--   OFFERING ENRICHMENT (added 2026-06-03):
--     Seven columns at the end of the SELECT carry the Net Gain offering
--     classification for each termination event:
--
--       department_code, department_id, department_name, offering, service,
--       is_offering_excluded, offering_source
--
--     Resolution chain (per event):
--       1. ADP point-in-time (`offering_source = 'adp_pit'`):
--          LATERAL into bronze.adp_tenure_history, latest snapshot where
--          (applicant_id = cu.applicant_id AND snapshot_date <= termination_date).
--          This reflects the department the employee was paid in during their
--          final pay period.
--       2. Onboarding department fallback (`offering_source = 'onboarding_dept'`):
--          udf.onboarding_department_id (from the user_department_fallback CTE)
--          when ADP returns nothing. Used for pre-2025 terminations and
--          never-started records.
--       3. NULL: no resolution available.
--
--     is_offering_excluded is the OFFERING-level exclusion (TRUE for
--     Interns / HRMS / Internal / Long-Term). Distinct from the existing
--     is_excluded column on this view (which is reason-based — TRUE for
--     termination_reason_id = 13 "Never started / delayed"). Net Gain
--     reports should filter BOTH:
--       WHERE is_excluded = FALSE
--         AND is_offering_excluded = FALSE
--
--   Duplicate user guard:
--                bronze.portal_users has ~588 cases where two records share an
--                applicant_id. canonical_users CTE resolves to one row per
--                applicant_id — prefers is_active = TRUE, then latest created_at.
--
--   Duplicate termination guard:
--                bronze.portal_terminations contains same-day double-submits
--                (~72 groups / ~76 extra rows) — the same termination entered
--                twice for one person on one day. terminations_deduped CTE
--                collapses these to one row per (user_id, termination_date),
--                keeping the most-recently-updated copy. Without it, COUNT(*)
--                over-counts terminations and Net Gain understates.
--
-- Portal 1 → Cajetan field mapping (reference for future UNION branch):
--   portal_requisitions.id               → orders.id / positions.id  (order_id / position_id)
--   portal_requisitions.requisition_key  → positions.legacy_requisition_id (position_key)
--   portal_requisitions.position         → positions.title            (position_title)
--   COALESCE(a.client_id, r_fb.client_id)→ clients.id                (client_id)
--   portal_clients.name                  → clients.name               (client_name)
--   portal_applicants.recruiter_id       → assignments.recruiter_id → users.id
--
-- Join key with other unified views:
--   v_terminations_unified.position_id = v_positions_unified.position_id
--   v_terminations_unified.order_id    = v_orders_unified.order_id
--   (both same value in Portal 1; diverge in Portal 2)
--
-- Caller examples:
--
--   -- Records ENTERED in the current ISO week (operational reporting)
--   SELECT * FROM silver.v_terminations_unified
--   WHERE entry_iso_year = EXTRACT(isoyear FROM CURRENT_DATE)
--     AND entry_iso_week = EXTRACT(week    FROM CURRENT_DATE)
--     AND is_excluded    = FALSE;
--
--   -- Records with termination_date in the current ISO week (event-anchored)
--   SELECT * FROM silver.v_terminations_unified
--   WHERE iso_year    = EXTRACT(isoyear FROM CURRENT_DATE)
--     AND iso_week    = EXTRACT(week    FROM CURRENT_DATE)
--     AND is_excluded = FALSE;
--
--   -- Rolling 4-week trend by entry week (operational view)
--   SELECT entry_iso_year, entry_iso_week, client_name, termination_category, COUNT(*)
--   FROM silver.v_terminations_unified
--   WHERE is_excluded = FALSE
--     AND termination_created_at >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '3 weeks'
--   GROUP BY 1, 2, 3, 4
--   ORDER BY 1, 2, 3, 4;
--
--   -- All conversions YTD (by entry date)
--   SELECT * FROM silver.v_terminations_unified
--   WHERE termination_category = 'CON'
--     AND is_excluded           = FALSE
--     AND termination_created_at >= DATE_TRUNC('year', CURRENT_DATE);
-- ============================================================================


-- ── Step 0: Index prerequisites ───────────────────────────────────────────
-- Run these before the view. CONCURRENTLY avoids locking the table.
-- Safe to re-run; IF NOT EXISTS is idempotent. The ADP index is shared with
-- v_hires_unified — first deploy of either view creates it.
--
-- The AJL index includes requisition_id as a covered column so the
-- ajl_fallback CTE can satisfy itself entirely from the index (no heap
-- fetches). Drops query time on the AJL fallback substantially.
--
-- ⚠ ON EXISTING DEPLOYMENTS: IF NOT EXISTS will SKIP creation when an index
-- of the same name already exists, even if the INCLUDE clause differs. To
-- pick up the INCLUDE change, build a new index alongside the old one and
-- swap them — see the file header for the migration steps.
--
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ajl_applicant_id
  ON bronze.portal_applicant_job_listings (applicant_id, created_at DESC)
  INCLUDE (requisition_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_adp_tenure_applicant_snap
  ON bronze.adp_tenure_history (applicant_id, snapshot_date);


-- ── Step 1: Deploy the view ───────────────────────────────────────────────
-- DROP + CREATE required because CREATE OR REPLACE cannot reorder/rename columns.
-- (No column changes in this revision — column list is identical to prior version.
--  Using DROP/CREATE as a precautionary pattern per project convention.)

DROP VIEW IF EXISTS silver.v_terminations_unified;

CREATE VIEW silver.v_terminations_unified AS

-- ── Portal 1 branch ───────────────────────────────────────────────────────
WITH

-- 0. Collapse duplicate termination records to one row per termination.
--    Portal 1 lets the same termination be submitted more than once for the
--    same person on the same day (a double-submit — consecutive ids, created
--    within seconds, otherwise identical). Those are data-entry duplicates,
--    NOT distinct terminations (you cannot be terminated from a placement
--    twice on the same calendar day), and they inflate every COUNT(*) over
--    this view. ~0.2% of rows, but it compounds the Net Gain math (terms are
--    subtracted from hires). Mirrors the same-day de-dup in v_hires_unified.
--    De-dupe key: (user_id, termination_date).
--    Keep the most-recently-UPDATED row (then highest id) — a handful of these
--    duplicates had the reason corrected on one copy after entry, and the
--    edited copy is the authoritative one. For identical double-submits the
--    tiebreak is moot.
terminations_deduped AS (
    SELECT *
    FROM (
        SELECT
            t.*,
            ROW_NUMBER() OVER (
                PARTITION BY t.user_id, t.termination_date
                ORDER BY t.updated_at DESC, t.id DESC
            ) AS dup_rn
        FROM bronze.portal_terminations t
    ) ranked
    WHERE dup_rn = 1
),

-- 1. Resolve duplicate portal_users → one canonical row per applicant_id.
--    Prefers is_active = TRUE, then most recently created record.
canonical_users AS (
    SELECT DISTINCT ON (applicant_id)
        id                                      AS user_id,
        applicant_id,
        first_name,
        last_name,
        first_name || ' ' || last_name          AS full_name,
        hire_date,
        is_active
    FROM bronze.portal_users
    WHERE applicant_id IS NOT NULL
    ORDER BY
        applicant_id,
        is_active   DESC,
        created_at  DESC
),

-- 2. Recruiter display names — recruiters are also portal_users rows.
recruiters AS (
    SELECT
        u.id,
        first_name || ' ' || last_name          AS recruiter_name
    FROM bronze.portal_users u
    INNER JOIN bronze.portal_role_user ru ON u.id = ru.user_id
    WHERE ru.role_id = 6
),

-- 3. AJL fallback: most recent job listing per applicant.
--    ONLY used when portal_applicants.client_id IS NULL (~97.6% of applicant rows).
--    The LEFT JOIN condition (a.client_id IS NULL) ensures this subquery is only
--    evaluated for rows that actually need it, keeping the primary path fast.
--    Requires idx_ajl_applicant_id on (applicant_id, created_at DESC).
ajl_fallback AS (
    SELECT DISTINCT ON (applicant_id)
        applicant_id,
        requisition_id
    FROM bronze.portal_applicant_job_listings
    ORDER BY
        applicant_id,
        created_at DESC
),

-- 4. User-department fallback: rescue terminations whose applicant chain
--    dead-ends (no a.client_id, no a.requisition_id, no AJL row).
--    Resolves client via portal_users.onboarding_department_id →
--    portal_departments.client_id. Joined by t.user_id at the bottom of
--    the FROM chain. Recon (last 120 days, is_excluded = false): rescues
--    ~22 of 562 non-excluded terminations otherwise left with NULL client.
--
--    Note: rescued records will still have NULL position_id, position_key,
--    position_title, is_pipeline, and recruiter_name — there is no
--    requisition for these by design. Tracked separately as a known limitation.
user_department_fallback AS (
    SELECT
        u.id                            AS user_id,
        u.onboarding_department_id,
        d.client_id                     AS dept_client_id,
        c.name                          AS dept_client_name
    FROM bronze.portal_users u
    LEFT JOIN bronze.portal_departments d ON d.id = u.onboarding_department_id
    LEFT JOIN bronze.portal_clients     c ON c.id = d.client_id
    WHERE u.onboarding_department_id IS NOT NULL
)

SELECT

    -- ── Source tracking ───────────────────────────────────────────────────
    'portal_v1'                                 AS source_system,

    -- ── Termination event ─────────────────────────────────────────────────
    t.id                                        AS termination_id,
    t.termination_date,
    t.last_worked_on,

    -- ── Reason / type hierarchy ───────────────────────────────────────────
    tr.id                                       AS termination_reason_id,
    tr.name                                     AS termination_reason,
    tt.id                                       AS termination_type_id,
    tt.name                                     AS termination_type,

    -- ── Derived reporting category ────────────────────────────────────────
    CASE
        WHEN tr.id = 13                         THEN 'EXCLUDED'
        WHEN tr.id = 1                          THEN 'CONV'
        WHEN tt.id = 1                          THEN 'VOL'
        WHEN tt.id = 2                          THEN 'INVOL'
        ELSE                                         'OTHER'
    END                                         AS termination_category,

    -- ── Exclusion flag ────────────────────────────────────────────────────
    (tr.id = 13)                                AS is_excluded,

    -- ── Free-text reason supplements ──────────────────────────────────────
    t.reason_other,
    t.personal_reason_description,

    -- ── Person ────────────────────────────────────────────────────────────
    cu.user_id,
    cu.applicant_id,
    cu.first_name,
    cu.last_name,
    cu.full_name,

    -- ── Position / Order context ──────────────────────────────────────────
    -- requisition_id: primary source is portal_applicants; fallback to AJL.
    COALESCE(a.requisition_id, ajl_fallback.requisition_id)
                                                AS position_id,
    COALESCE(a.requisition_id, ajl_fallback.requisition_id)
                                                AS order_id,
    r.requisition_key                           AS position_key,
    r.position                                  AS position_title,
    r.is_pipeline,

    -- ── Client ────────────────────────────────────────────────────────────
    -- client_id resolution priority (2026-07-08 point-in-time fix):
    --   1. v_department_offering.client_id  (vdo — client of the ADP point-in-
    --      time / onboarding department resolved for the offering enrichment
    --      below; the client the associate actually worked for at termination)
    --   2. portal_applicants.client_id      (application-era; used when the
    --      department chain does not resolve)
    --   3. portal_requisitions.client_id    (via AJL fallback; PIPELINE reqs
    --      EXCLUDED — a pipeline requisition's client is a bench label, not a
    --      real placement)
    --   4. portal_departments.client_id     (via portal_users.onboarding_department_id)
    -- Final NULL only if every source fails.
    -- c (portal_clients) is joined below on the pipeline-aware applicant/
    -- requisition id, so c.name always matches tiers 2–3; vdo and udf each carry
    -- their own matched id/name pair — so client_id and client_name always
    -- resolve from the SAME tier.
    COALESCE(
        vdo.client_id,
        a.client_id,
        CASE WHEN NOT COALESCE(r.is_pipeline, FALSE) THEN r.client_id END,
        udf.dept_client_id
    )                                           AS client_id,
    COALESCE(vdo.client_name, c.name, udf.dept_client_name) AS client_name,

    -- ── Tenure ────────────────────────────────────────────────────────────
    -- EPISODE-BOUND hire_date (2026-07-01): canonical_users.hire_date is ONE
    -- mutable value per applicant (the CURRENT stint, overwritten on rehire). If
    -- it POSTDATES this termination it belongs to a LATER episode, not the stint
    -- this row ends — pairing them produced negative tenure (applicant 136818's
    -- 2017 exit was stamped with a 2026 start => tenure_days -3310). When the
    -- stored start postdates the termination it is treated as UNMATCHED: hire_date
    -- and tenure_days are NULL (the older stint's true start was overwritten and
    -- is not recoverable here). tenure_days can therefore never be negative.
    CASE WHEN cu.hire_date <= t.termination_date
         THEN cu.hire_date END                  AS hire_date,
    CASE
        WHEN cu.hire_date IS NOT NULL
         AND t.termination_date IS NOT NULL
         AND cu.hire_date <= t.termination_date
        THEN (t.termination_date - cu.hire_date)::int
        ELSE NULL
    END                                         AS tenure_days,

    CASE
        WHEN cu.hire_date IS NULL
          OR t.termination_date IS NULL
          OR cu.hire_date > t.termination_date  THEN 'Unknown'
        WHEN (t.termination_date - cu.hire_date) <  90  THEN 'Short'
        WHEN (t.termination_date - cu.hire_date) < 365  THEN 'Mid'
        ELSE                                             'Long'
    END                                         AS tenure_band,

    -- ── Recruiter ─────────────────────────────────────────────────────────
    a.recruiter_id,
    rec.recruiter_name,

    -- ── ISO week helpers: TERMINATION DATE axis ───────────────────────────
    -- Use for event-anchored reporting (official last day of work).
    EXTRACT(isoyear FROM t.termination_date)::int   AS iso_year,
    EXTRACT(week    FROM t.termination_date)::int   AS iso_week,

    -- ── Record entry date + ISO week helpers: ENTRY DATE axis ────────────
    -- termination_created_at: when the record was entered in the portal.
    -- entry_iso_year / entry_iso_week: primary filters for operational
    -- "entered this week" reporting. Terminations are frequently entered
    -- days after the event; use this axis for current-workload views.
    t.created_at                                AS termination_created_at,
    EXTRACT(isoyear FROM t.created_at)::int     AS entry_iso_year,
    EXTRACT(week    FROM t.created_at)::int     AS entry_iso_week,

    -- ── Entry lag (convenience field) ────────────────────────────────────
    -- Days between official termination date and portal entry.
    -- 0 = entered same day. Negative values indicate termination_date
    -- is set in the future (e.g., future-dated "never started" records).
    (t.created_at::date - t.termination_date)   AS entry_lag_days,

    -- ── Audit ─────────────────────────────────────────────────────────────
    -- record_created_at retained for backward compatibility.
    -- Identical to termination_created_at; prefer termination_created_at
    -- in new queries.
    t.created_at                                AS record_created_at,
    t.updated_at                                AS record_updated_at,

    -- ── Offering enrichment ───────────────────────────────────────────────
    -- See header "OFFERING ENRICHMENT" for the full resolution story.
    -- Resolved department code first (ADP PIT at termination, then onboarding
    -- fallback from user_department_fallback CTE); everything else is joined
    -- from v_department_offering on department_code.
    COALESCE(adp_pit.dept_code, onboard_dept.code)  AS department_code,
    vdo.department_id,
    vdo.department_name,
    vdo.offering,
    vdo.service,
    COALESCE(vdo.is_excluded, FALSE)                AS is_offering_excluded,
    CASE
        WHEN adp_pit.dept_code  IS NOT NULL THEN 'adp_pit'
        WHEN onboard_dept.code  IS NOT NULL THEN 'onboarding_dept'
        ELSE                                         NULL
    END                                             AS offering_source

FROM terminations_deduped t

INNER JOIN bronze.portal_termination_reasons tr
    ON tr.id = t.termination_reason_id
INNER JOIN bronze.portal_termination_types tt
    ON tt.id = tr.termination_type_id

INNER JOIN canonical_users cu
    ON cu.user_id = t.user_id
INNER JOIN bronze.portal_applicants a
    ON a.id = cu.applicant_id

-- AJL fallback: only join when portal_applicants has no client/requisition.
-- The a.client_id IS NULL condition short-circuits this join for the ~2.4%
-- of applicant rows where client_id IS populated, keeping the primary path fast.
LEFT JOIN ajl_fallback
    ON ajl_fallback.applicant_id = a.id
   AND a.client_id IS NULL

-- Resolve requisition: primary from portal_applicants, fallback from AJL.
LEFT JOIN bronze.portal_requisitions r
    ON r.id = COALESCE(a.requisition_id, ajl_fallback.requisition_id)

-- Resolve the applicant/requisition-tier client name. Pipeline-aware: a
-- pipeline requisition's client is a bench/pipeline label, not a real
-- placement, so it is excluded here (and from client_id above). c.id therefore
-- equals tiers 2–3 of the client_id COALESCE, keeping c.name aligned with it.
LEFT JOIN bronze.portal_clients c
    ON c.id = COALESCE(a.client_id,
                       CASE WHEN NOT COALESCE(r.is_pipeline, FALSE) THEN r.client_id END)

-- User-department fallback: rescues terminations whose applicant chain
-- dead-ends (no a.client_id, no r.client_id). Joined unconditionally —
-- the COALESCE in the SELECT picks the higher-priority source when
-- multiple resolve. Also supplies onboarding_department_id for the
-- offering-enrichment fallback below.
LEFT JOIN user_department_fallback udf
    ON udf.user_id = t.user_id

LEFT JOIN recruiters rec
    ON rec.id = a.recruiter_id

-- ── Offering enrichment joins ─────────────────────────────────────────────
-- 1. ADP point-in-time: latest snapshot at or before the termination_date
--    for the applicant. Reflects the department where they were paid in
--    their final pay period. Requires idx_adp_tenure_applicant_snap.
LEFT JOIN LATERAL (
    SELECT TRIM(adp.home_department_code)       AS dept_code
    FROM bronze.adp_tenure_history adp
    WHERE adp.applicant_id   = cu.applicant_id
      AND adp.snapshot_date <= t.termination_date
    ORDER BY adp.snapshot_date DESC
    LIMIT 1
) adp_pit ON TRUE

-- 2. Onboarding department fallback. udf.onboarding_department_id is already
--    resolved by the user_department_fallback CTE above — just expose the
--    portal_departments.code for the offering join.
LEFT JOIN bronze.portal_departments onboard_dept
    ON onboard_dept.id = udf.onboarding_department_id

-- 3. Offering crosswalk. Joined by the resolved department code; vdo is
--    NULL for both columns when neither ADP nor onboarding resolve.
LEFT JOIN silver.v_department_offering vdo
    ON vdo.department_code = COALESCE(adp_pit.dept_code, onboard_dept.code)


-- ══════════════════════════════════════════════════════════════════════════
-- FUTURE: Cajetan (Portal 2) UNION ALL branch
-- Add when cajetan schema is available in the warehouse.
-- Column list and order must exactly match the portal_v1 branch above.
-- New fields to add to Cajetan branch:
--   t.created_at   AS termination_created_at,
--   EXTRACT(isoyear FROM t.created_at)::int  AS entry_iso_year,
--   EXTRACT(week    FROM t.created_at)::int  AS entry_iso_week,
--   (t.created_at::date - t.termination_date) AS entry_lag_days,
--   t.created_at   AS record_created_at,   -- backward compat alias
-- ══════════════════════════════════════════════════════════════════════════
--
-- UNION ALL
-- SELECT
--     'cajetan'::text                               AS source_system,
--     t.id                                          AS termination_id,
--     ... (all fields matching portal_v1 branch) ...
--     t.created_at                                  AS termination_created_at,
--     EXTRACT(isoyear FROM t.created_at)::int       AS entry_iso_year,
--     EXTRACT(week    FROM t.created_at)::int       AS entry_iso_week,
--     (t.created_at::date - t.termination_date)     AS entry_lag_days,
--     t.created_at                                  AS record_created_at,
--     t.updated_at                                  AS record_updated_at
-- FROM cajetan.terminations t
-- ...
;


COMMENT ON VIEW silver.v_terminations_unified IS
'Unified terminations view. One row per termination event.
Uses Cajetan (Portal 2) vocabulary throughout; Portal 1 fields aliased to match.

TWO DATE AXES:
  termination_date / iso_year / iso_week
    Official last day of work. Use for tenure, client attrition, historical trends.
  termination_created_at / entry_iso_year / entry_iso_week
    Date record was entered in the portal. Use for operational "entered this week"
    reporting. Terminations are often entered days after the event (typical lag
    1–5 days; max observed 80+ days).

entry_lag_days: (termination_created_at::date - termination_date). Convenience
field for late-entry analysis. Negative = future-dated record.

record_created_at: backward-compat alias for termination_created_at. Prefer
termination_created_at in new queries.

CLIENT RESOLUTION (revised 2026-07-08 — point-in-time fix):
  1. v_department_offering.client_id — client of the ADP point-in-time /
     onboarding department (the client worked at termination; ADP = system of record)
  2. portal_applicants.client_id     — application-era fallback
  3. portal_requisitions.client_id   — via AJL fallback; PIPELINE reqs EXCLUDED
  4. portal_departments.client_id    — via onboarding_department_id
  5. NULL                            — true orphan; no source resolves
  Prior (pre-2026-07-08) behavior led with application-era data, so stale
  applicant records and Nationwide pipeline reqs mis-attributed the client.
  ⚠ CLIENT FAMILIES: report filters must use a client_id SET, not one name —
    Univar = {567,577,581,591}; Nationwide has PA / S&S variants.
  Requires: idx_ajl_applicant_id on portal_applicant_job_listings(applicant_id, created_at DESC)

Portal 1: position_id = order_id (1:1 — same requisition).
Portal 2: position_id ≠ order_id (1:many — add UNION ALL branch when Cajetan is live).

Key filters callers should apply:
  AND is_excluded = FALSE           -- removes "Never started / delayed"
  AND is_pipeline = FALSE           -- removes pipeline/bench reqs (decision pending)
  AND is_offering_excluded = FALSE  -- removes Interns / HRMS / Internal / Long-Term

Join keys:
  position_id → v_positions_unified.position_id
  order_id    → v_orders_unified.order_id
  (source_system, position_id) for cross-view joins

OFFERING ENRICHMENT (2026-06-03):
  department_code / department_id / department_name / offering / service /
  is_offering_excluded / offering_source

  Resolved by:
    1. adp_pit          (bronze.adp_tenure_history, latest snapshot <= termination_date)
    2. onboarding_dept  (udf.onboarding_department_id from user_department_fallback CTE)
    3. NULL

  is_offering_excluded = TRUE for Interns / HRMS / Internal / Long-Term;
  Net Gain reports should filter is_offering_excluded = FALSE in ADDITION TO
  is_excluded = FALSE (those track different exclusion concepts).

  Requires: idx_adp_tenure_applicant_snap on
            bronze.adp_tenure_history (applicant_id, snapshot_date).
  Source rejected: portal_associates.id is NOT a reliable proxy for
                   portal_users.id (~32% of associate rows collide with
                   unrelated user rows from different time periods).';


-- ============================================================================
-- VALIDATION QUERIES
-- Run after deploying to confirm correctness.
-- ============================================================================

-- 1. Row count — should be unchanged from prior version
--    SELECT COUNT(*) FROM silver.v_terminations_unified;
--    SELECT COUNT(*) FROM bronze.portal_terminations;

-- 2. No duplicate termination_ids within a source_system
--    SELECT source_system, termination_id, COUNT(*)
--    FROM silver.v_terminations_unified
--    GROUP BY 1, 2
--    HAVING COUNT(*) > 1;

-- 3. Bryant fix verification — should now show Goodyear Flex - Fayetteville NC
--    SELECT termination_id, full_name, client_name, position_title, recruiter_name
--    FROM silver.v_terminations_unified
--    WHERE termination_id = '27842';

-- 4. Client resolution improvement check
--    SELECT
--      COUNT(*) AS total,
--      COUNT(client_name) AS with_client,
--      COUNT(*) - COUNT(client_name) AS still_null,
--      ROUND(100.0 * COUNT(client_name) / COUNT(*), 1) AS pct_resolved
--    FROM silver.v_terminations_unified
--    WHERE is_excluded = FALSE;

-- 5. Category distribution — should be unchanged
--    SELECT termination_category, COUNT(*)
--    FROM silver.v_terminations_unified
--    GROUP BY 1
--    ORDER BY 2 DESC;

-- 6. Entry lag distribution — spot-check late entry patterns
--    SELECT
--      CASE
--        WHEN entry_lag_days < 0  THEN 'Future-dated'
--        WHEN entry_lag_days = 0  THEN 'Same day'
--        WHEN entry_lag_days <= 3 THEN '1–3 days'
--        WHEN entry_lag_days <= 7 THEN '4–7 days'
--        ELSE '8+ days'
--      END AS lag_bucket,
--      COUNT(*)
--    FROM silver.v_terminations_unified
--    WHERE is_excluded = FALSE
--    GROUP BY 1
--    ORDER BY MIN(entry_lag_days);

-- 7. Current entry week spot-check (operational view)
--    SELECT full_name, client_name, termination_date, termination_created_at::date,
--           entry_lag_days, termination_category, termination_reason,
--           recruiter_name, tenure_days, tenure_band,
--           is_pipeline, is_excluded
--    FROM silver.v_terminations_unified
--    WHERE entry_iso_year = EXTRACT(isoyear FROM CURRENT_DATE)
--      AND entry_iso_week = EXTRACT(week    FROM CURRENT_DATE)
--      AND is_excluded    = FALSE
--    ORDER BY termination_created_at DESC, client_name, full_name;

-- 8. Late entry flag — records entered this week with termination_date in a prior week
--    SELECT full_name, client_name, termination_date, termination_created_at::date,
--           entry_lag_days, termination_category
--    FROM silver.v_terminations_unified
--    WHERE entry_iso_year = EXTRACT(isoyear FROM CURRENT_DATE)
--      AND entry_iso_week = EXTRACT(week    FROM CURRENT_DATE)
--      AND is_excluded    = FALSE
--      AND entry_lag_days > 6
--    ORDER BY entry_lag_days DESC;

-- 9. Offering resolution coverage — what % of recent terminations get an offering
--    SELECT
--      COUNT(*)                                                    AS total,
--      COUNT(offering)                                             AS with_offering,
--      COUNT(*) FILTER (WHERE offering_source = 'adp_pit')         AS via_adp,
--      COUNT(*) FILTER (WHERE offering_source = 'onboarding_dept') AS via_onboarding,
--      COUNT(*) FILTER (WHERE offering_source IS NULL)             AS unresolved,
--      ROUND(100.0 * COUNT(offering) / COUNT(*), 1)                AS pct_with_offering
--    FROM silver.v_terminations_unified
--    WHERE termination_date >= CURRENT_DATE - INTERVAL '90 days'
--      AND is_excluded = FALSE;

-- 10. Offering distribution on Net Gain eligible terminations (last 90 days)
--    SELECT offering, service, termination_category, COUNT(*)
--    FROM silver.v_terminations_unified
--    WHERE termination_date >= CURRENT_DATE - INTERVAL '90 days'
--      AND is_excluded          = FALSE
--      AND is_offering_excluded = FALSE
--    GROUP BY 1, 2, 3
--    ORDER BY 1, 4 DESC;

-- 11. Episode-binding guard (2026-07-01) — tenure_days can never be negative.
--     Expect zero rows. Also: hire_date is NULL exactly when it would postdate
--     the termination (unmatched episode).
--    SELECT COUNT(*) AS negative_tenure_rows
--    FROM silver.v_terminations_unified WHERE tenure_days < 0;

-- 12. Fixture 136818 — the 2017 exit no longer shows tenure_days -3310;
--     hire_date is NULL (2026 start postdates the 2017 termination).
--    SELECT termination_id, termination_date, hire_date, tenure_days, tenure_band
--    FROM silver.v_terminations_unified WHERE applicant_id = 136818
--    ORDER BY termination_date;