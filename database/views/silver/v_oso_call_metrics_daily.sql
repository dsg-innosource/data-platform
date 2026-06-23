-- ============================================================================
-- Silver Layer View: v_oso_call_metrics_daily
-- Purpose: Unified daily telephony metrics per OSO client (AEP, Alliant Energy)
--          for the OSO executive dashboard.
-- Created: 2026-06-04
-- Design:  docs/oso-silver-views-design.md
-- ============================================================================
-- Grain: one row per client per day.
--
-- Source semantics (validated 2026-06-04):
--   AEP — bronze.aep_oso_daily_data, which is a bronze VIEW rolling up
--   bronze.aep_oso_half_hour_data by date (the half-hour reports are the
--   client-agreed source, confirmed w/ Jourdan 2026-06-04; rollup verified
--   identical to the decimal). total_answer_speed and total_handle_time are
--   TOTAL seconds across calls (handle_time/accepted ≈ 269–290s, matching the
--   Metabase "AEP OSO Average Handle Time" gauge and the <300s SLA), so
--   ASA/AHT are derived by dividing by accepted calls. Known issue: the
--   half-hour ingest is gappy and lags (max 2026-05-14 as of writing).
--
--   Alliant — bronze.alliant_energy_daily_aht (agent-day grain, aggregated to
--   day). "Handle Time" is TOTAL seconds per agent-day; weighted AHT =
--   SUM(Handle Time) / SUM(Handled) ≈ 470–490s weekly.
--
--   Alliant calls_offered / calls_abandoned / abandon_rate / asa_seconds are
--   intentionally NULL: the only source (bronze.alliant_energy_repeat_callers)
--   stopped loading 2025-07-31. Columns are kept so the view shape is stable
--   when a contact-detail feed returns (deferred per 2026-06-04 decision).
-- ============================================================================

CREATE OR REPLACE VIEW silver.v_oso_call_metrics_daily AS

-- AEP: program-level daily rollup, already one row per day
SELECT
    'AEP'::text                                         AS client_name,
    'aep_oso'::text                                     AS source_system,
    d."Date"::date                                      AS metric_date,
    d.total_calls::bigint                               AS calls_offered,
    d.total_accepted_calls::bigint                      AS calls_handled,
    d.abandoned::bigint                                 AS calls_abandoned,
    ROUND(d.abandoned::numeric
          / NULLIF(d.total_calls, 0), 4)                AS abandon_rate,
    ROUND(d.total_answer_speed::numeric
          / NULLIF(d.total_accepted_calls, 0), 1)       AS asa_seconds,
    ROUND(d.total_handle_time::numeric
          / NULLIF(d.total_accepted_calls, 0), 1)       AS aht_seconds
FROM bronze.aep_oso_daily_data d

UNION ALL

-- Alliant Energy: agent-day grain aggregated to day
SELECT
    'Alliant Energy'::text                              AS client_name,
    'alliant_energy'::text                              AS source_system,
    a."Login Date"::date                                AS metric_date,
    NULL::bigint                                        AS calls_offered,   -- deferred (stale contact feed)
    SUM(a."Handled")::bigint                            AS calls_handled,
    NULL::bigint                                        AS calls_abandoned, -- deferred
    NULL::numeric                                       AS abandon_rate,    -- deferred
    NULL::numeric                                       AS asa_seconds,     -- deferred
    ROUND(SUM(a."Handle Time")::numeric
          / NULLIF(SUM(a."Handled"), 0), 1)             AS aht_seconds
FROM bronze.alliant_energy_daily_aht a
GROUP BY a."Login Date";

-- ============================================================================
-- Documentation
-- ============================================================================

COMMENT ON VIEW silver.v_oso_call_metrics_daily IS
'Unified daily telephony metrics per OSO client. One row per client per day. '
'Clients: AEP (full metrics), Alliant Energy (AHT + handled only — '
'offered/abandoned/ASA are NULL until the Alliant contact-detail feed returns; '
'always guard with IS NOT NULL or COALESCE when aggregating). All *_seconds '
'columns are per-call averages in seconds; abandon_rate is a 0–1 fraction. '
'Weekly rollups should use silver.v_oso_weekly_summary instead of re-deriving.';

COMMENT ON COLUMN silver.v_oso_call_metrics_daily.client_name IS 'OSO client: ''AEP'' or ''Alliant Energy''. Matches department_reporting_map.client for headcount joins.';
COMMENT ON COLUMN silver.v_oso_call_metrics_daily.source_system IS 'Bronze source family: aep_oso or alliant_energy.';
COMMENT ON COLUMN silver.v_oso_call_metrics_daily.metric_date IS 'Grain (with client_name). Calendar day of the call activity. AEP loads can lag 1–3 weeks; check MAX(metric_date) per client before assuming completeness.';
COMMENT ON COLUMN silver.v_oso_call_metrics_daily.calls_offered IS 'Total calls offered (incl. abandoned). NULL for Alliant Energy (deferred — stale contact feed).';
COMMENT ON COLUMN silver.v_oso_call_metrics_daily.calls_handled IS 'Calls accepted/handled by agents. Alliant includes inbound + outbound handled.';
COMMENT ON COLUMN silver.v_oso_call_metrics_daily.calls_abandoned IS 'Calls abandoned in queue. NULL for Alliant Energy (deferred).';
COMMENT ON COLUMN silver.v_oso_call_metrics_daily.abandon_rate IS 'calls_abandoned / calls_offered, 0–1 fraction (multiply by 100 for %). NULL for Alliant Energy.';
COMMENT ON COLUMN silver.v_oso_call_metrics_daily.asa_seconds IS 'Average speed of answer, seconds per handled call (AEP: total_answer_speed / accepted). NULL for Alliant Energy (deferred).';
COMMENT ON COLUMN silver.v_oso_call_metrics_daily.aht_seconds IS 'Average handle time, seconds per handled call (talk + hold + wrap). Weight by calls_handled when re-aggregating across days. CAUTION: the Alliant value is platform-derived (NICE daily_aht) and does not match the client-agreed source (intraday report, not yet in warehouse) — do not present as the client-facing Alliant AHT.';
