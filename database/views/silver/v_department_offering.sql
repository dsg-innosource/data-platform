-- ============================================================================
-- Silver Layer View: v_department_offering
-- Purpose: Crosswalk between bronze.portal_departments and the Data Governance
--          "offering" classification used by Net Gain reporting. One row per
--          department code, with the offering dimension joined in from
--          silver.department_reporting_map.
-- Created:  2026-06-03
-- Modified: 2026-07-08 — Added `reporting_client` — the curated client ROLLUP
--           from silver.department_reporting_map.client. This is the normalized
--           reporting family (e.g. "Univar Solutions" collapses the portal
--           entities Monument Consulting Univar, …AZ, …CA, and even NEXEO/
--           SUPERIOR into one label). Distinct from client_id/client_name, which
--           remain the granular *portal* client entity. Consuming views
--           (v_hires_unified, v_terminations_unified, v_hire_retention) surface
--           it so client-level "how is <client> doing" questions resolve the
--           whole family via the curated map instead of a fragile name match.
--           Same join/grain as offering/service (no fan-out); NULL for codes not
--           in department_reporting_map, exactly like offering.
-- Modified: 2026-06-03 — Two fixes after first deploy:
--           (1) Dedup bronze.portal_departments by `code` to prevent fan-out
--               in consuming views. portal_departments has 11 codes with two
--               rows each (active + inactive variants); the prior view exposed
--               both, producing duplicate (event_id, termination_id) rows
--               when joined.
--           (2) Include department_reporting_map rows whose department_number
--               does not exist in portal_departments. 19 map rows are
--               affected — notably ALL Long-Term (8) and Internal (5) codes,
--               which are ADP-only department buckets that were never
--               provisioned in the portal. Without this, Long-Term and
--               Internal employees silently land in Net Gain instead of being
--               excluded.
--
--           Uses DROP VIEW ... CASCADE because v_hires_unified and
--           v_terminations_unified now depend on this view; a plain DROP
--           would fail with a dependency error. CASCADE drops the dependents
--           as a side effect, which is fine because both unified views are
--           recreated immediately after as part of the same deploy. See the
--           DEPLOYMENT ORDER block below.
-- ============================================================================
-- DESIGN NOTES:
--   Grain      : One row per department_code.
--
--   Source     : UNION of two streams:
--     A. bronze.portal_departments (deduped by code, prefers is_active = TRUE
--        then highest id as tiebreaker) LEFT JOIN
--        silver.department_reporting_map (offering / service)
--     B. silver.department_reporting_map rows whose department_number has no
--        matching portal_departments row (map orphans — typically ADP-only
--        department codes for Long-Term, Internal, etc.).
--
--   Map orphans surface with:
--     department_id  = NULL  (no portal_departments row exists)
--     client_id      = NULL
--     client_name    = NULL
--     department_name = m.department_name  (from the map)
--     offering / service / is_excluded as normal
--
--   Offering values currently observed in department_reporting_map:
--     Net Gain offerings:      Contract (212), Flex (257), OSO (38)
--     Excluded from Net Gain:  Interns (14), Long-Term (8), Internal (5), HRMS (4)
--
--   is_excluded   : TRUE when offering is one of
--                   ('Interns', 'HRMS', 'Internal', 'Long-Term').
--                   Consuming views surface this as is_offering_excluded and
--                   Net Gain reporting should filter is_offering_excluded = FALSE.
--
-- ----------------------------------------------------------------------------
-- USAGE
-- ----------------------------------------------------------------------------
--   Joined from v_hires_unified and v_terminations_unified by department_code:
--
--     LEFT JOIN silver.v_department_offering vdo
--         ON vdo.department_code = <resolved_dept_code>
--
--   The consuming view resolves the department code itself (ADP point-in-time
--   first, onboarding fallback second), then joins here for the offering.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- DEPLOYMENT ORDER
-- ----------------------------------------------------------------------------
-- v_hires_unified and v_terminations_unified depend on this view, and
-- v_hire_retention depends on BOTH of those. The CASCADE on the DROP below
-- therefore drops all THREE dependents as a side effect. You MUST redeploy
-- every dependent immediately after running this file, in this order:
--
--   1. database/views/silver/v_department_offering.sql   (this file — CASCADE drops all dependents)
--   2. database/views/silver/v_hires_unified.sql         (recreates v_hires_unified)
--   3. database/views/silver/v_terminations_unified.sql  (recreates v_terminations_unified)
--   4. database/views/silver/v_hire_retention.sql        (recreates v_hire_retention — depends on 2 & 3)
--   5. database/views/silver/v_active_roster.sql         (recreates v_active_roster — depends on this view)
--
-- If you do not run steps 2–5, the dependent views will be missing until you
-- do. Existing queries against them will fail with "relation does not exist"
-- until they're recreated. (Step 4 was added 2026-06-29 when v_hire_retention
-- was introduced; CASCADE drops it too.)
-- ----------------------------------------------------------------------------

DROP VIEW IF EXISTS silver.v_department_offering CASCADE;

CREATE VIEW silver.v_department_offering AS
WITH dedup_portal_depts AS (
    -- One row per code; prefer the active row, then the highest id as tiebreaker.
    -- 11 codes in portal_departments have two rows; this collapses them so the
    -- consuming join can't fan out.
    SELECT DISTINCT ON (code)
        id,
        code,
        name,
        client_id
    FROM bronze.portal_departments
    WHERE code IS NOT NULL
    ORDER BY
        code,
        is_active DESC NULLS LAST,
        id        DESC
),
combined AS (
    -- Stream A: every (deduped) portal department, joined to its map row if any.
    SELECT
        d.id                                AS department_id,
        d.code                              AS department_code,
        d.name                              AS department_name,
        d.client_id,
        c.name                              AS client_name,
        m.offering,
        m.service,
        m.client                            AS reporting_client
    FROM dedup_portal_depts                 d
    LEFT JOIN bronze.portal_clients         c ON c.id = d.client_id
    LEFT JOIN silver.department_reporting_map m ON m.department_number = d.code

    UNION ALL

    -- Stream B: map rows whose department_number has no portal_departments row.
    -- Rescues Long-Term / Internal / orphaned HRMS codes that would otherwise
    -- silently fail to classify their employees.
    -- NULL casts are character varying (not text) to keep the UNION ALL output
    -- types identical to Stream A — required by CREATE OR REPLACE VIEW.
    SELECT
        NULL::bigint                        AS department_id,
        m.department_number                 AS department_code,
        m.department_name                   AS department_name,
        NULL::bigint                        AS client_id,
        NULL::character varying             AS client_name,
        m.offering,
        m.service,
        m.client                            AS reporting_client
    FROM silver.department_reporting_map    m
    WHERE NOT EXISTS (
        SELECT 1 FROM dedup_portal_depts d WHERE d.code = m.department_number
    )
)
SELECT
    department_id,
    department_code,
    department_name,
    client_id,
    client_name,
    reporting_client,
    offering,
    service,
    COALESCE(
        offering IN ('Interns', 'HRMS', 'Internal', 'Long-Term'),
        FALSE
    )                                       AS is_excluded
FROM combined;


COMMENT ON VIEW silver.v_department_offering IS
'Crosswalk between portal_departments and the Data Governance offering
classification (Contract / Flex / OSO / Interns / Long-Term / Internal / HRMS).
One row per department code. Combines two streams:
  (A) deduped portal_departments LEFT JOIN department_reporting_map
  (B) department_reporting_map rows orphaned from portal_departments
      (ADP-only codes — Long-Term, Internal, some HRMS — that exist in payroll
       but were never provisioned in the portal)

Map orphans have department_id, client_id, client_name = NULL but their
offering / service / is_excluded are populated normally.

reporting_client (2026-07-08): the curated client ROLLUP from
department_reporting_map.client (e.g. "Univar Solutions" collapses Monument
Consulting Univar / …AZ / …CA / NEXEO-SUPERIOR into one label). Use this for
client-family reporting; client_name is the granular portal entity. NULL for
codes absent from department_reporting_map, same as offering.

is_excluded: TRUE when the offering is one of Interns / Long-Term / Internal / HRMS
(not included in Net Gain). Consuming views surface this as is_offering_excluded;
Net Gain reporting should filter is_offering_excluded = FALSE.

Used by: v_hires_unified, v_terminations_unified for offering enrichment.';


-- ============================================================================
-- VALIDATION QUERIES
-- ============================================================================

-- 1. Row count — should be (distinct portal codes) + (orphaned map rows)
--    SELECT COUNT(*) FROM silver.v_department_offering;
--    SELECT
--      (SELECT COUNT(DISTINCT code) FROM bronze.portal_departments WHERE code IS NOT NULL) AS distinct_portal,
--      (SELECT COUNT(*) FROM silver.department_reporting_map m
--         WHERE NOT EXISTS (SELECT 1 FROM bronze.portal_departments d WHERE d.code = m.department_number)) AS orphan_map;

-- 2. Uniqueness — must be 1 row per department_code (consuming views depend on it)
--    SELECT COUNT(*) FROM (
--      SELECT department_code FROM silver.v_department_offering
--      GROUP BY 1 HAVING COUNT(*) > 1
--    ) x;

-- 3. All map rows surface — no offering classification should be missing
--    SELECT offering, COUNT(*)
--    FROM silver.v_department_offering
--    WHERE offering IS NOT NULL
--    GROUP BY 1 ORDER BY 2 DESC;
--    -- Expect to see all 7 offerings: Flex, Contract, OSO, Interns, Long-Term, Internal, HRMS

-- 4. Verify exclusion logic
--    SELECT offering, is_excluded, COUNT(*)
--    FROM silver.v_department_offering
--    WHERE offering IS NOT NULL
--    GROUP BY 1, 2 ORDER BY 1;

-- 5. Spot-check orphan rescue — these codes should now resolve
--    SELECT * FROM silver.v_department_offering
--    WHERE department_code IN ('001501', '001435', '001440', '001432')
--    ORDER BY department_code;
