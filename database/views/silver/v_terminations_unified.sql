-- ============================================================================
-- Silver Layer View: v_terminations_unified
-- Purpose: Present Portal 1.0 termination records in Portal 2.0 structure
-- Created:  2026-03-13
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
--
-- ⚠ PREREQUISITE — run this index before deploying the view:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ajl_applicant_id
--     ON bronze.portal_applicant_job_listings (applicant_id, created_at DESC);
--   Without this index, the ajl_fallback CTE will seq-scan a 1 GB table.
-- ============================================================================
-- DESIGN NOTES:
--   Grain      : One row per termination event
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
--   Client resolution priority (portal_v1 branch):
--     1. portal_applicants.client_id         — set at placement; authoritative
--     2. portal_applicant_job_listings        — most recent AJL row; fallback
--        (only joined when portal_applicants.client_id IS NULL)
--     3. NULL                                 — true orphan; no placement data
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
--   Duplicate user guard:
--                bronze.portal_users has ~588 cases where two records share an
--                applicant_id. canonical_users CTE resolves to one row per
--                applicant_id — prefers is_active = TRUE, then latest created_at.
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


-- ── Step 0: Index prerequisite ────────────────────────────────────────────
-- Run this before the view. CONCURRENTLY avoids locking the table.
-- Safe to re-run; IF NOT EXISTS is idempotent.
--
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ajl_applicant_id
  ON bronze.portal_applicant_job_listings (applicant_id, created_at DESC);


-- ── Step 1: Deploy the view ───────────────────────────────────────────────
-- DROP + CREATE required because CREATE OR REPLACE cannot reorder/rename columns.
-- (No column changes in this revision — column list is identical to prior version.
--  Using DROP/CREATE as a precautionary pattern per project convention.)

DROP VIEW IF EXISTS silver.v_terminations_unified;

CREATE VIEW silver.v_terminations_unified AS

-- ── Portal 1 branch ───────────────────────────────────────────────────────
WITH

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
    -- client_id: portal_applicants is authoritative when populated.
    -- Falls back to the client on the resolved requisition (via AJL).
    COALESCE(a.client_id, r.client_id)          AS client_id,
    c.name                                      AS client_name,

    -- ── Tenure ────────────────────────────────────────────────────────────
    cu.hire_date,
    CASE
        WHEN cu.hire_date IS NOT NULL
         AND t.termination_date IS NOT NULL
        THEN (t.termination_date - cu.hire_date)::int
        ELSE NULL
    END                                         AS tenure_days,

    CASE
        WHEN cu.hire_date IS NULL
          OR t.termination_date IS NULL         THEN 'Unknown'
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
    t.updated_at                                AS record_updated_at

FROM bronze.portal_terminations t

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

-- Resolve client: primary from portal_applicants, fallback from requisition
-- (which was resolved via AJL above).
LEFT JOIN bronze.portal_clients c
    ON c.id = COALESCE(a.client_id, r.client_id)

LEFT JOIN recruiters rec
    ON rec.id = a.recruiter_id


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

CLIENT RESOLUTION (2026-03-17):
  Primary:  portal_applicants.client_id (authoritative when populated; ~2.4% of rows)
  Fallback: most recent portal_applicant_job_listings row → portal_requisitions.client_id
            (covers ~14.7% of terminations otherwise left with NULL client_name)
  Orphan:   ~14.9% of terminations have no client data in either source
  Requires: idx_ajl_applicant_id on portal_applicant_job_listings(applicant_id, created_at DESC)

Portal 1: position_id = order_id (1:1 — same requisition).
Portal 2: position_id ≠ order_id (1:many — add UNION ALL branch when Cajetan is live).

Key filters callers should apply:
  AND is_excluded = FALSE   -- removes "Never started / delayed"
  AND is_pipeline = FALSE   -- removes pipeline/bench reqs (decision pending)

Join keys:
  position_id → v_positions_unified.position_id
  order_id    → v_orders_unified.order_id
  (source_system, position_id) for cross-view joins';


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