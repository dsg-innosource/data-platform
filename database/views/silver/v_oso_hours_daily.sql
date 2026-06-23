-- ============================================================================
-- Silver Layer View: v_oso_hours_daily
-- Purpose: OPERATIONAL attendance/login hours per OSO client/agent/day.
-- Created: 2026-06-04
-- Modified: 2026-06-04 — DEMOTED from exec-dashboard hours basis to
--           operational data only. Source validation (ad-hoc w/ Jourdan)
--           established that the executive "Hours (Week)" metric is ADP
--           payroll-processed/invoiced hours, which are NOT in the warehouse
--           yet (separate ingest project — see design doc findings). The
--           attendance/login hours here run materially higher than the exec
--           packet (Alliant ~6,300/wk vs ~3,180) and must not feed exec
--           hours reporting. Still useful for schedule adherence, absence
--           analysis, and agent-level operational views.
-- Design:  docs/oso-silver-views-design.md
-- ============================================================================
-- Grain: one row per client per agent per day per activity bucket
--        (Alliant carries activity_code through for auditability; AEP is one
--        'Login Session' bucket per agent-day).
--
-- Source semantics (validated 2026-06-04):
--   Alliant — bronze.alliant_energy_attendance is segment-based: each agent-day
--   is partitioned into non-overlapping time segments (Activity/Shift "Open",
--   Activity "Lunch", "Training--New Hire", ...). SUM(paid_hours) per agent-day
--   across all rows is therefore safe (no double counting). Lunch/Break carry
--   0 paid hours. Historical rows also used Type='Shift'; recent data uses
--   'Activity' and 'Activity/Shift' — the classification below keys on
--   activity code only, not Type.
--
--   AEP — bronze.aep_oso_login session rows (start/end timestamps; verified
--   no NULL or negative ends; ≈7.8 hrs per agent-day). Hours = summed session
--   duration. NOTE: this is logged-in time, the closest available proxy for
--   paid/worked hours on AEP until a true schedule/payroll feed exists.
--
-- is_worked flag:
--   FALSE for absence/time-off codes (even when paid) and unpaid segments
--   (Lunch/Break/Split Shift); TRUE for everything else (Open, Overtime,
--   Training*, Project*, Meeting*, Mentoring, Equipment*, Make Up Time, ...).
--   The dashboard's "Total Hours (Week)" sums is_worked = TRUE only.
--   Code list to be reviewed with ops — adjust the CASE below, not downstream.
-- ============================================================================

CREATE OR REPLACE VIEW silver.v_oso_hours_daily AS

-- Alliant Energy: attendance segments
SELECT
    'Alliant Energy'::text                       AS client_name,
    'alliant_energy'::text                       AS source_system,
    a."Agent Name"                               AS agent_name,
    a."Scheduled Date"::date                     AS work_date,
    a."Activity Code"                            AS activity_code,
    CASE
        WHEN a."Activity Code" IN ('Lunch', 'Break', 'Split Shift')          THEN FALSE
        WHEN a."Activity Code" LIKE 'Unplanned--%'                           THEN FALSE
        WHEN a."Activity Code" LIKE 'Planned--%'                             THEN FALSE
        WHEN a."Activity Code" IN ('Paid Time Off (PTO)',
                                   'Holiday',
                                   'Full Week Vacation Bid',
                                   'Time Off Own Account',
                                   'Jury Duty',
                                   'Administrative Leave')                   THEN FALSE
        ELSE TRUE
    END                                          AS is_worked,
    SUM(a.paid_hours)::numeric(10, 2)            AS hours
FROM bronze.alliant_energy_attendance a
GROUP BY a."Agent Name", a."Scheduled Date", a."Activity Code"

UNION ALL

-- AEP: login session durations per agent-day
SELECT
    'AEP'::text                                  AS client_name,
    'aep_oso'::text                              AS source_system,
    l.agent_name                                 AS agent_name,
    l.start_timestamp::date                      AS work_date,
    'Login Session'::text                        AS activity_code,
    TRUE                                         AS is_worked,
    ROUND((SUM(EXTRACT(EPOCH FROM (l.end_timestamp - l.start_timestamp)))
           / 3600.0)::numeric, 2)                AS hours
FROM bronze.aep_oso_login l
WHERE l.end_timestamp IS NOT NULL
  AND l.end_timestamp >= l.start_timestamp
GROUP BY l.agent_name, l.start_timestamp::date;

-- ============================================================================
-- Documentation
-- ============================================================================

COMMENT ON VIEW silver.v_oso_hours_daily IS
'OPERATIONAL attendance/login hours per OSO client/agent/day/activity bucket. '
'NOT the executive "Hours (Week)" metric — that is ADP payroll/invoiced hours, '
'not yet in the warehouse (separate project; these hours run materially '
'higher). Use for schedule adherence and absence analysis only. Grain: client '
'+ agent + day + activity_code (Alliant keeps attendance activity codes; AEP '
'is a single ''Login Session'' bucket per agent-day). Sum hours WHERE '
'is_worked for productive time; is_worked = FALSE rows are paid absence '
'(PTO/holiday/FMLA) and unpaid segments. Sources: Alliant = attendance '
'segments (non-overlapping, safe to sum); AEP = login session durations. '
'Agent names are source-native strings — join via v_oso_roster_unified for IDs.';

COMMENT ON COLUMN silver.v_oso_hours_daily.client_name IS 'OSO client: ''AEP'' or ''Alliant Energy''. Matches department_reporting_map.client.';
COMMENT ON COLUMN silver.v_oso_hours_daily.source_system IS 'Bronze source family: aep_oso (login sessions) or alliant_energy (attendance).';
COMMENT ON COLUMN silver.v_oso_hours_daily.agent_name IS 'Agent name as it appears in the source. Alliant: ''First Last'' (attendance format). AEP: Pulse export name. Not a stable key — prefer roster IDs for joins.';
COMMENT ON COLUMN silver.v_oso_hours_daily.work_date IS 'Calendar day. Alliant = scheduled date; AEP = session start date.';
COMMENT ON COLUMN silver.v_oso_hours_daily.activity_code IS 'Alliant attendance activity code (Open, Overtime, Training--*, Project--*, Paid Time Off (PTO), Unplanned--*, ...). AEP rows are always ''Login Session''.';
COMMENT ON COLUMN silver.v_oso_hours_daily.is_worked IS 'TRUE = productive/worked time (the dashboard''s hours basis). FALSE = absence/time-off (even when paid) and unpaid segments (Lunch/Break). Classification rule lives in this view — adjust here, not downstream.';
COMMENT ON COLUMN silver.v_oso_hours_daily.hours IS 'Hours for the bucket. Alliant = SUM(paid_hours) (0 for unpaid segments like Lunch); AEP = summed login session duration.';
