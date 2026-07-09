-- ============================================================================
-- Silver Layer View: v_active_roster
-- Purpose: Point-in-time roster of CURRENTLY ACTIVE associates with tenure, as
--          of the latest ADP snapshot. Answers "who is on-site right now and how
--          long have they been here" — by client, department, offering, tenure
--          band — WITHOUT the trailing-window cap that scopes v_hire_retention.
-- Created:  2026-07-09
--
-- WHY THIS EXISTS (vs v_hire_retention):
--   v_hire_retention is a retention-CHECKPOINT surface — one row per hire START
--   in the trailing 365 days, matched to its earliest termination, built to
--   answer "of associates who reached the 6/8/15-week mark, how many are still
--   active?" Its 365-day cap is on actual_start_date and is correct for that
--   purpose, but it therefore EXCLUDES anyone who started >1 year ago — i.e.
--   most of the long-tenured active population. Using it for "active headcount /
--   tenure" undercounts badly (e.g. Goodyear InfoLink showed 8 of 30 active).
--   This view is the right source for the active-population question: no start
--   cap, sourced from the payroll system of record (ADP).
--
--   fact_active_headcount is the AGGREGATE (active_count per department per
--   snapshot); this is the per-associate DETAIL behind it.
-- ============================================================================
-- DESIGN NOTES:
--   Grain      : One row per ACTIVE ADP position at the latest snapshot.
--                position_id is unique per row (natural key). A person with two
--                concurrent active positions (dual assignment) gets two rows —
--                rare (1 case observed); count DISTINCT applicant_id for people.
--
--   As-of      : Point-in-time. Every row is from the single latest
--                snapshot_date in bronze.adp_tenure_history (exposed as
--                as_of_date). ADP loads weekly, so this reflects the most recent
--                load — NOT a live feed. Callers wanting a trend read
--                fact_active_headcount or the raw snapshots.
--
--   Source     : bronze.adp_tenure_history — the payroll system of record for
--                who is active and their hire_date. ADP-native by design:
--                ~18% of active rows have NULL applicant_id (associates not
--                linked to a portal applicant), so name/department/client come
--                from ADP directly and applicant_id is a NULLABLE link out to
--                portal-based silver views (v_hires_unified, v_terminations_
--                unified) when present.
--
--   Client resolution (two independent sources, both exposed):
--     1. v_department_offering (client_id / client_name / reporting_client /
--        offering / service) — resolved from the ADP home_department_code, so it
--        matches how v_hires_unified / v_terminations_unified attribute client.
--        NULL when the department code is not in the reporting map (~12% of
--        active rows — the unmapped-dept gap; see DSG "Goodyear Findlay
--        departments" and department_reporting_map).
--     2. adp_client / adp_client_code — the raw ADP payroll client. Always
--        present when ADP populated it; covers depts missing from the reporting
--        map. Use reporting_client for curated family reporting; adp_client as a
--        completeness fallback. Rows where BOTH are NULL (~12%) are genuinely
--        unattributed in ADP (clientless/unmapped department).
--
--   Tenure     : tenure_days = CURRENT_DATE - hire_date. tenure_band matches the
--                sibling views: Short (<90d), Mid (90-364d), Long (>=365d).
--                hire_date is always populated for active rows (0 NULLs
--                observed) so 'Unknown' should not occur, but is handled.
--
--   PII        : Excludes regular_pay_rate and home_phone (sensitive). Exposes
--                payroll_name and email (consistent with v_oso_roster_unified).
--
--   Status scope: position_status = 'Active' only. Leave-of-absence and other
--                statuses are excluded; add a status filter here if the business
--                wants LOA counted as active.
--
-- ----------------------------------------------------------------------------
-- CALLER EXAMPLES
-- ----------------------------------------------------------------------------
--   -- Active tenure breakdown for a client (the InfoLink question)
--   SELECT tenure_band, COUNT(*), ROUND(AVG(tenure_days)/30.0, 1) AS avg_months
--   FROM silver.v_active_roster
--   WHERE adp_department_description ILIKE '%InfoLink%'
--   GROUP BY tenure_band ORDER BY MIN(tenure_days);
--
--   -- Active headcount + avg tenure by reporting client
--   SELECT reporting_client, COUNT(*) AS active, ROUND(AVG(tenure_days)/30.0,1) AS avg_months
--   FROM silver.v_active_roster
--   WHERE is_offering_excluded = FALSE
--   GROUP BY 1 ORDER BY 2 DESC;
--
--   -- Departments active in ADP but missing from the reporting map (data gap)
--   SELECT DISTINCT department_code, adp_department_description
--   FROM silver.v_active_roster WHERE reporting_client IS NULL;
--
-- DEPLOYMENT: depends on silver.v_department_offering. A future
--   v_department_offering redeploy (DROP ... CASCADE) will drop this view too;
--   recreate it after the unified views in that deploy sequence.
-- ============================================================================

DROP VIEW IF EXISTS silver.v_active_roster;

CREATE VIEW silver.v_active_roster AS
WITH latest_snapshot AS (
    -- Single most-recent ADP load; the whole roster is anchored to it.
    SELECT MAX(snapshot_date) AS snapshot_date
    FROM bronze.adp_tenure_history
)
SELECT
    -- ── Source / as-of ─────────────────────────────────────────────────────
    'adp'                                       AS source_system,
    a.snapshot_date                             AS as_of_date,

    -- ── Person (ADP-native; applicant_id is a NULLABLE link out) ───────────
    a.applicant_id,
    a.payroll_name,
    a.email,
    a.file_number,
    a.adp_id,
    a.position_id,
    a.business_unit,

    -- ── Tenure ─────────────────────────────────────────────────────────────
    a.hire_date,
    a.rehire_date,
    (CURRENT_DATE - a.hire_date)::int           AS tenure_days,
    CASE
        WHEN a.hire_date IS NULL                       THEN 'Unknown'
        WHEN (CURRENT_DATE - a.hire_date) <  90        THEN 'Short'
        WHEN (CURRENT_DATE - a.hire_date) <  365       THEN 'Mid'
        ELSE                                                'Long'
    END                                         AS tenure_band,

    -- ── Department ─────────────────────────────────────────────────────────
    TRIM(a.home_department_code)                AS department_code,
    a.home_department_description               AS adp_department_description,

    -- ── Client via v_department_offering (matches hires/terminations) ──────
    vdo.department_name,
    vdo.client_id,
    vdo.client_name,
    vdo.reporting_client,
    vdo.offering,
    vdo.service,
    COALESCE(vdo.is_excluded, FALSE)            AS is_offering_excluded,

    -- ── Client via raw ADP (completeness fallback for unmapped depts) ──────
    a.client                                    AS adp_client,
    a.client_code                               AS adp_client_code,

    -- ── Recruiting attribution ─────────────────────────────────────────────
    a.recruited_by

FROM bronze.adp_tenure_history a
INNER JOIN latest_snapshot ls
    ON ls.snapshot_date = a.snapshot_date
-- 1 row per department_code in v_department_offering, so this cannot fan out.
LEFT JOIN silver.v_department_offering vdo
    ON vdo.department_code = TRIM(a.home_department_code)
WHERE a.position_status = 'Active';


COMMENT ON VIEW silver.v_active_roster IS
'Point-in-time roster of currently-active associates with tenure, as of the
latest ADP snapshot (as_of_date). One row per active ADP position
(position_id unique; dual assignments = two rows — COUNT(DISTINCT applicant_id)
for people). Sourced from bronze.adp_tenure_history (payroll system of record).

Use this for active-population / tenure questions ("who is on-site now and how
long"). Do NOT use v_hire_retention for that — it is capped to starts in the
trailing 365 days (a retention-checkpoint surface) and excludes long-tenured
actives. fact_active_headcount is the aggregate count; this is the detail.

ADP-native: ~18% of active rows have NULL applicant_id (not linked to a portal
applicant); name/department/client come from ADP and applicant_id is a nullable
join key to v_hires_unified / v_terminations_unified.

Client: reporting_client / client_name / client_id resolve via
v_department_offering from the ADP home_department_code (matches how hires and
terminations attribute client); NULL for departments absent from the reporting
map. adp_client / adp_client_code are the raw ADP client (completeness fallback).

Tenure bands match the sibling views: Short <90d, Mid 90-364d, Long >=365d.
PII: regular_pay_rate and home_phone are intentionally excluded.
Scope: position_status = ''Active'' only (LOA and other statuses excluded).';


-- ============================================================================
-- VALIDATION QUERIES
-- ============================================================================

-- 1. Grain — position_id unique, one snapshot only
--    SELECT COUNT(*) rows, COUNT(DISTINCT position_id) positions,
--           COUNT(DISTINCT as_of_date) snapshots
--    FROM silver.v_active_roster;   -- rows = positions, snapshots = 1

-- 2. Active headcount vs fact_active_headcount aggregate (sanity)
--    SELECT COUNT(*) FROM silver.v_active_roster;
--    SELECT SUM(active_count) FROM silver.fact_active_headcount
--    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM silver.fact_active_headcount);

-- 3. Goodyear InfoLink fixture — expect ~30 active, ~22 Long, avg ~27 months
--    SELECT tenure_band, COUNT(*), ROUND(AVG(tenure_days)/30.0,1) avg_months
--    FROM silver.v_active_roster
--    WHERE adp_department_description ILIKE '%InfoLink%'
--    GROUP BY 1 ORDER BY MIN(tenure_days);

-- 4. Unmapped-department data gap — depts active in ADP but not in reporting map
--    SELECT department_code, adp_department_description, adp_client, COUNT(*)
--    FROM silver.v_active_roster WHERE reporting_client IS NULL
--    GROUP BY 1,2,3 ORDER BY 4 DESC;
