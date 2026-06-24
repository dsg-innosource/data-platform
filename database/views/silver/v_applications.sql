-- =============================================================================
-- silver.v_applications  —  Application-event spine (top of the recruiting funnel)
-- =============================================================================
-- PURPOSE
--   Surfaces the TRUE application event, which silver did not previously expose.
--   The existing funnel view (silver.v_applicant_activity_events) is sourced from
--   portal_requisition_statistics and starts at Phone Screen (type 2); the "applied"
--   event is not in that table. This view lands it from bronze.portal_applicant_job_listings,
--   reusing the canonical logic from the Metabase model DP001 (application_dates CTE).
--
-- GRAIN
--   One row per (applicant_id, requisition_id) = one application.
--   application_at = MIN(created_at) across that pair's job-listing rows (first apply).
--   application_event_count exposes re-applies without changing the grain.
--
-- KEY DESIGN DECISIONS (settle/confirm with Sean)
--   1. ALL-TIME: DP001's `fill_by >= 2024` cutoff is intentionally NOT applied here,
--      for parity with the rest of the silver funnel (data goes back to 2015).
--   2. is_pipeline comes straight from bronze.portal_requisitions (r.is_pipeline) —
--      same source as v_applicant_activity_events. No join to v_positions_unified
--      (that view only re-exposes r.is_pipeline) and no requisition_key heuristic.
--   3. ISO coords use EXTRACT(ISOYEAR/WEEK). Confirm this matches the exact expression
--      v_applicant_activity_events uses so weekly rollups line up.
--   4. No test/junk filtering applied (mirrors the funnel view). Add if a flag exists.
--   5. ALL applications retained: req/client/applicant are LEFT-joined, so an
--      application is never dropped when its req or client can't be matched
--      (mirrors v_applicant_activity_events). client_id falls back to the
--      applicant's client_id when the req has none.
--   6. JOB-LISTING ENRICHMENT (2026-06-24): the actual posting the applicant applied
--      to is carried via the EARLIEST application row's job_listing_id ->
--      bronze.portal_job_listings. Exposes job_title (100% populated, the real posted
--      title). categories is NOT exposed: its values mix function + industry (Customer
--      Service, Manufacturing, Finance & Accounting, ...) and do not map to the four
--      reporting functions, plus it is only ~60% populated. job_description is also NOT
--      exposed: ~1.3KB of HTML per row, duplicated across the applicant x req grain, and
--      invites expensive ILIKE scans / SELECT * bloat. Do description- or category-level
--      work as an ad-hoc bronze join.
--
-- JOIN KEYS
--   order_id = position_id = requisition_id (identity in Portal v1; Portal v2 will fan out).
--   Joins to silver.v_applicant_activity_events / v_hires_unified / v_terminations_unified
--   on (applicant_id, order_id) and to v_orders_unified / v_positions_unified on position_id.
--
-- CHANGE LOG
--   2026-06-24 - Initial version. Source: bronze.portal_applicant_job_listings.
--   2026-06-24 - Added job-listing enrichment: job_listing_id, job_title
--                (from the earliest application's bronze.portal_job_listings row).
-- =============================================================================

DROP VIEW IF EXISTS silver.v_applications;

CREATE OR REPLACE VIEW silver.v_applications AS
WITH listings AS (
    SELECT
        jl.applicant_id,
        jl.requisition_id,
        MIN(jl.created_at)                                                  AS application_at,
        MAX(jl.created_at)                                                  AS last_application_at,
        COUNT(*)                                                            AS application_event_count,
        (ARRAY_AGG(jl.id             ORDER BY jl.created_at, jl.id))[1]     AS application_id,
        (ARRAY_AGG(jl.job_listing_id ORDER BY jl.created_at, jl.id))[1]     AS job_listing_id,   -- earliest apply's posting
        (ARRAY_AGG(jl.is_from_api    ORDER BY jl.created_at, jl.id))[1]     AS is_from_api,
        BOOL_OR(jl.is_internal_move)                                        AS is_internal_move
    FROM bronze.portal_applicant_job_listings jl
    WHERE jl.requisition_id IS NOT NULL
      AND jl.applicant_id   IS NOT NULL
    GROUP BY jl.applicant_id, jl.requisition_id
),
source_attr AS (   -- source of the EARLIEST application for the pair (mirrors DP001 application_sources)
    SELECT DISTINCT ON (jl.applicant_id, jl.requisition_id)
           jl.applicant_id,
           jl.requisition_id,
           COALESCE(s.name, 'Unknown')                                      AS source_name
    FROM bronze.portal_applicant_job_listings jl
    LEFT JOIN bronze.portal_applicant_sources s ON s.id = jl.applicant_source_id
    WHERE jl.requisition_id IS NOT NULL
      AND jl.applicant_id   IS NOT NULL
    ORDER BY jl.applicant_id, jl.requisition_id, jl.created_at, jl.id
)
SELECT
    'portal_v1'::text                                                       AS source_system,
    l.application_id,
    l.applicant_id,
    a.first_name                                                            AS applicant_first_name,
    a.last_name                                                             AS applicant_last_name,
    (a.first_name || ' ' || a.last_name)                                    AS applicant_full_name,
    l.requisition_id                                                        AS order_id,      -- = position_id in Portal v1
    l.requisition_id                                                        AS position_id,
    r.requisition_key,                                                                        -- legacy key (ADP joins)
    r.requisition_key                                                       AS position_key,  -- Portal v2 / funnel-join naming
    r.position                                                              AS position_title,    -- requisition role (portal_requisitions.position)
    l.job_listing_id,                                                                         -- earliest apply's posting (portal_job_listings.id)
    jlst.job_title,                                                                           -- actual posted title (100% populated)
    COALESCE(r.client_id, a.client_id)                                      AS client_id,     -- mirrors v_applicant_activity_events
    c.name                                                                  AS client_name,
    l.application_at,
    l.application_at::date                                                  AS application_date,        -- canonical filter axis
    EXTRACT(ISOYEAR FROM l.application_at)::int                             AS application_iso_year,
    EXTRACT(WEEK    FROM l.application_at)::int                             AS application_iso_week,
    TO_CHAR(l.application_at, 'IYYY-"W"IW')                                 AS iso_week_label,
    sa.source_name,
    l.application_event_count,
    l.last_application_at,
    l.is_internal_move,
    l.is_from_api,
    r.is_pipeline
FROM listings l
-- All source joins LEFT so we never drop an application: keep it even when the req or
-- client can't be matched (mirrors v_applicant_activity_events). applicants and
-- requisitions are joined before clients so client_id can fall back to either.
LEFT JOIN bronze.portal_applicants    a    ON a.id   = l.applicant_id
LEFT JOIN bronze.portal_requisitions  r    ON r.id   = l.requisition_id
LEFT JOIN bronze.portal_clients       c    ON c.id   = COALESCE(r.client_id, a.client_id)
LEFT JOIN bronze.portal_job_listings  jlst ON jlst.id = l.job_listing_id    -- 1:1 with job_listing_id
LEFT JOIN source_attr                 sa   ON sa.applicant_id = l.applicant_id
                                          AND sa.requisition_id = l.requisition_id;

COMMENT ON VIEW silver.v_applications IS
'Application-event spine — the TRUE "applied" event at the top of the recruiting
funnel, which silver did not previously expose (v_applicant_activity_events starts
at Phone Screen). Landed from bronze.portal_applicant_job_listings using the
canonical DP001 logic. All-time (no fill_by cutoff), no test/junk filtering.

Grain: one row per (applicant_id, requisition_id) = one application;
application_at = first apply, application_event_count exposes re-applies.

Enrichment: job_title comes from the earliest apply''s portal_job_listings row
(job_listing_id). categories and job_description are deliberately not exposed
(category mixes function/industry and is sparse; description is large HTML) — use
an ad-hoc bronze join for that.

Join key: (applicant_id, order_id) to the funnel/hire/term views,
order_id to v_orders_unified / v_positions_unified. Portal 1: order_id =
position_id = requisition_id (1:1).';

-- =============================================================================
-- POST-DEPLOY VALIDATION
-- =============================================================================
-- 1) All-time totals (expect first_app ~2015):
--    SELECT COUNT(*) AS applications, COUNT(DISTINCT applicant_id) AS people,
--           MIN(application_date) AS first_app, MAX(application_date) AS last_app
--    FROM silver.v_applications;
--
-- 2) AEP reconciliation — must equal the validated Metabase/job-listings numbers:
--    expect applications = 317763, people = 231210
--    SELECT COUNT(*) AS applications, COUNT(DISTINCT applicant_id) AS people
--    FROM silver.v_applications
--    WHERE client_name ILIKE '%aep%';
--
-- 2b) Unmatched rows retained by the LEFT joins (now kept, not dropped):
--    SELECT COUNT(*) FILTER (WHERE requisition_key IS NULL) AS no_req,
--           COUNT(*) FILTER (WHERE client_name    IS NULL) AS no_client
--    FROM silver.v_applications;
--
-- 2c) Job-listing enrichment coverage (expect job_title ~100%):
--    SELECT COUNT(*) AS rows,
--           COUNT(job_listing_id)                              AS has_listing,
--           COUNT(*) FILTER (WHERE job_title IS NOT NULL)      AS has_title
--    FROM silver.v_applications;
--
-- 3) Applied -> screened -> hired conversion for a client (join to the existing funnel):
--    WITH app AS (
--      SELECT order_id, COUNT(*) AS applications
--      FROM silver.v_applications WHERE client_name ILIKE '%aep%' GROUP BY order_id),
--    scr AS (
--      SELECT order_id, COUNT(DISTINCT applicant_id) AS screened
--      FROM silver.v_applicant_activity_events
--      WHERE event_category = 'PHONE_SCREEN' AND client_name ILIKE '%aep%' GROUP BY order_id)
--    SELECT SUM(app.applications) AS applications, SUM(scr.screened) AS screened,
--           ROUND(100.0*SUM(scr.screened)/NULLIF(SUM(app.applications),0),1) AS screen_rate_pct
--    FROM app LEFT JOIN scr USING (order_id);
-- =============================================================================