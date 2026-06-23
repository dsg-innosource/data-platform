-- ============================================================================
-- Silver Layer View: v_oso_weekly_summary
-- Purpose: Client × ISO-week rollup feeding the OSO executive dashboard
--          (per-client SLA cards: ASA / AHT / abandon % / weekly hours).
-- Created: 2026-06-04
-- Design:  docs/oso-silver-views-design.md
-- ============================================================================
-- Grain: one row per client per ISO week.
--
-- DEPENDS ON (deploy first):
--   silver.v_oso_call_metrics_daily
--   silver.v_oso_hours_daily
--
-- ASA / AHT are volume-weighted: daily per-call averages are multiplied back
-- by calls_handled and re-divided by the weekly handled total, which exactly
-- reconstructs total-seconds / total-calls for the week.
--
-- total_hours_worked sums is_worked = TRUE only (paid/worked-hours decision,
-- 2026-06-04); total_hours_paid includes paid absence (PTO/holiday/etc.) for
-- reference.
--
-- Targets / GREEN-YELLOW status are intentionally absent — deferred until the
-- canonical targets source is located (planned: silver.oso_sla_targets seed
-- table; the dashboard joins it to this view).
--
-- FULL OUTER JOIN: a client-week appears if it has call metrics OR hours
-- (e.g. a holiday week with hours but a missed telephony load still shows).
-- ============================================================================

CREATE OR REPLACE VIEW silver.v_oso_weekly_summary AS

WITH calls_weekly AS (
    SELECT
        client_name,
        EXTRACT(ISOYEAR FROM metric_date)::int          AS iso_year,
        EXTRACT(WEEK    FROM metric_date)::int          AS iso_week,
        MIN(date_trunc('week', metric_date))::date      AS week_start,
        SUM(calls_offered)                              AS calls_offered,
        SUM(calls_handled)                              AS calls_handled,
        SUM(calls_abandoned)                            AS calls_abandoned,
        ROUND(SUM(calls_abandoned)::numeric
              / NULLIF(SUM(calls_offered), 0), 4)       AS abandon_rate,
        ROUND(SUM(asa_seconds * calls_handled)::numeric
              / NULLIF(SUM(calls_handled)
                       FILTER (WHERE asa_seconds IS NOT NULL), 0), 1) AS asa_seconds,
        ROUND(SUM(aht_seconds * calls_handled)::numeric
              / NULLIF(SUM(calls_handled)
                       FILTER (WHERE aht_seconds IS NOT NULL), 0), 1) AS aht_seconds,
        COUNT(*)                                        AS call_metric_days,
        MAX(metric_date)                                AS last_call_metric_date
    FROM silver.v_oso_call_metrics_daily
    GROUP BY client_name,
             EXTRACT(ISOYEAR FROM metric_date),
             EXTRACT(WEEK    FROM metric_date)
),

hours_weekly AS (
    SELECT
        client_name,
        EXTRACT(ISOYEAR FROM work_date)::int            AS iso_year,
        EXTRACT(WEEK    FROM work_date)::int            AS iso_week,
        MIN(date_trunc('week', work_date))::date        AS week_start,
        SUM(hours) FILTER (WHERE is_worked)             AS total_hours_worked,
        SUM(hours)                                      AS total_hours_paid,
        COUNT(DISTINCT agent_name)
            FILTER (WHERE is_worked AND hours > 0)      AS agents_worked,
        MAX(work_date)                                  AS last_hours_date
    FROM silver.v_oso_hours_daily
    GROUP BY client_name,
             EXTRACT(ISOYEAR FROM work_date),
             EXTRACT(WEEK    FROM work_date)
)

SELECT
    COALESCE(c.client_name, h.client_name)              AS client_name,
    COALESCE(c.iso_year,  h.iso_year)                   AS iso_year,
    COALESCE(c.iso_week,  h.iso_week)                   AS iso_week,
    COALESCE(c.week_start, h.week_start)                AS week_start,

    -- telephony
    c.calls_offered,
    c.calls_handled,
    c.calls_abandoned,
    c.abandon_rate,
    c.asa_seconds,
    c.aht_seconds,

    -- hours
    h.total_hours_worked,
    h.total_hours_paid,
    h.agents_worked,

    -- data-quality breadcrumbs (freshness varies by source; see design doc)
    c.call_metric_days,
    c.last_call_metric_date,
    h.last_hours_date

FROM calls_weekly c
FULL OUTER JOIN hours_weekly h
    ON  c.client_name = h.client_name
    AND c.iso_year    = h.iso_year
    AND c.iso_week    = h.iso_week;

-- ============================================================================
-- Documentation
-- ============================================================================

COMMENT ON VIEW silver.v_oso_weekly_summary IS
'Client × ISO-week rollup for the OSO executive dashboard. One row per client '
'per ISO week; the primary feed for per-client SLA cards (ASA / AHT / '
'abandon %). ASA and AHT are volume-weighted weekly averages. Hours columns '
'are PROVISIONAL (attendance/login-based, not the exec ADP-hours metric — '
'feed pending) and Alliant aht_seconds is platform-derived, not the '
'client-agreed source; see column comments before using either. '
'The current/in-progress week is always partial, and AEP telephony can lag '
'1–3 weeks — use call_metric_days and last_* columns to judge completeness, '
'and prefer the latest COMPLETE week for headline numbers. Alliant '
'offered/abandon/ASA are NULL (deferred — stale contact feed). Headcount and '
'net gain are intentionally NOT here: use silver.fact_active_headcount JOIN '
'silver.department_reporting_map (offering = ''OSO''). Targets/status are '
'also not here (pending silver.oso_sla_targets).';

COMMENT ON COLUMN silver.v_oso_weekly_summary.client_name IS 'OSO client: ''AEP'' or ''Alliant Energy''. Matches department_reporting_map.client.';
COMMENT ON COLUMN silver.v_oso_weekly_summary.iso_year IS 'ISO year of the week (grain, with client_name + iso_week). Pairs with iso_week — beware year boundaries.';
COMMENT ON COLUMN silver.v_oso_weekly_summary.iso_week IS 'ISO week number (1–53).';
COMMENT ON COLUMN silver.v_oso_weekly_summary.week_start IS 'Monday of the ISO week.';
COMMENT ON COLUMN silver.v_oso_weekly_summary.calls_offered IS 'Weekly total calls offered. NULL for Alliant Energy (deferred).';
COMMENT ON COLUMN silver.v_oso_weekly_summary.calls_handled IS 'Weekly total calls handled.';
COMMENT ON COLUMN silver.v_oso_weekly_summary.calls_abandoned IS 'Weekly total abandoned. NULL for Alliant Energy (deferred).';
COMMENT ON COLUMN silver.v_oso_weekly_summary.abandon_rate IS 'Weekly calls_abandoned / calls_offered, 0–1 fraction. NULL for Alliant Energy.';
COMMENT ON COLUMN silver.v_oso_weekly_summary.asa_seconds IS 'Volume-weighted average speed of answer (seconds). NULL for Alliant Energy (deferred).';
COMMENT ON COLUMN silver.v_oso_weekly_summary.aht_seconds IS 'Volume-weighted average handle time (seconds). Sanity range: AEP ~270–290, Alliant ~470–490 (May 2026). CAUTION: Alliant value is platform-derived and does not match the client-agreed source (intraday report, pending ingest) — exclude from client-facing/executive reporting until that feed lands.';
COMMENT ON COLUMN silver.v_oso_weekly_summary.total_hours_worked IS 'PROVISIONAL — attendance/login-based worked hours (v_oso_hours_daily.is_worked = TRUE). NOT the executive "Hours (Week)" metric, which is ADP payroll/invoiced hours (feed pending, separate project). Runs materially higher than the exec packet. Operational use only.';
COMMENT ON COLUMN silver.v_oso_weekly_summary.total_hours_paid IS 'PROVISIONAL — all attendance/login paid hours incl. paid absence. Same caveat as total_hours_worked. For AEP equals total_hours_worked (login-based source has no absence rows).';
COMMENT ON COLUMN silver.v_oso_weekly_summary.agents_worked IS 'Distinct agents with worked hours > 0 in the week. Name-based count, not headcount — see view comment.';
COMMENT ON COLUMN silver.v_oso_weekly_summary.call_metric_days IS 'Days with telephony data in the week (7 = complete for Alliant, 5–7 typical for AEP). < 5 usually means a partial/lagged load.';
COMMENT ON COLUMN silver.v_oso_weekly_summary.last_call_metric_date IS 'Latest telephony date contributing to the week. Freshness breadcrumb.';
COMMENT ON COLUMN silver.v_oso_weekly_summary.last_hours_date IS 'Latest hours date contributing to the week. Freshness breadcrumb.';
