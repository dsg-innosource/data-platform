-- ============================================================================
-- Silver Layer View: v_associate_hours_by_pay_period
-- Purpose: Cleaned associate-grain payroll hours over the ADP pay-period feed
--          (bronze.assoc_hours_by_pay_period), joined to the department
--          reporting map for client/offering. General-purpose: the OSO
--          dashboard rolls it up via v_oso_payroll_hours_by_period; other
--          dashboards can build their own rollups on this view.
-- Created: 2026-07-20
-- Design:  docs/oso-silver-views-design.md (hours addendum)
-- ============================================================================
-- Grain: one row per associate x home department x pay period x pay date.
--        Verified unique on (applicant_id, department, period_start, pay_date).
--
-- Source notes (profiled 2026-07-20, 24,773 rows, no NULLs in any column):
--   - AEP (dept 112998) is paid WEEKLY, Mon-Sun periods == ISO weeks.
--     Dept 112996 is the legacy AEP department (bi-weekly, Jan-Feb 2023 only).
--   - Alliant Energy runs TWO bi-weekly cycles offset by one day
--     (110307 Sun-Sat, 110308 Sat-Fri) that share the same pay date.
--   - Hours are as-paid, incl. corrections: a handful of rows carry negative
--     components (payroll reversals). Keep them - they net totals correctly.
--   - Other_Hours is paid absence (PTO/holiday - it spikes exactly on holiday
--     weeks while Regular drops). hours_paid = regular + overtime + other.
--   - The bronze feed is a MANUAL load for now (no scheduled ingest); check
--     MAX(period_end) for freshness before trusting recent weeks.
-- ============================================================================

CREATE OR REPLACE VIEW silver.v_associate_hours_by_pay_period AS

SELECT
    b.applicant_id,
    b."First Name"                                   AS first_name,
    b."Last Name"                                    AS last_name,
    b."Home Department Code"::text                   AS department_number,
    drm.client,
    drm.offering,
    b."Period_Beginning_Date"                        AS period_start,
    b."Period_End_Date"                              AS period_end,
    b."Pay_Date"                                     AS pay_date,
    (b."Period_End_Date" - b."Period_Beginning_Date" + 1) AS period_days,
    b."Regular_Hours"                                AS hours_regular,
    b."Overtime_Hours"                               AS hours_overtime,
    b."Other_Hours"                                  AS hours_other,
    (b."Regular_Hours" + b."Overtime_Hours" + b."Other_Hours") AS hours_paid
FROM bronze.assoc_hours_by_pay_period b
LEFT JOIN silver.department_reporting_map drm
       ON drm.department_number = b."Home Department Code"::text;

-- ============================================================================
-- Documentation
-- ============================================================================

COMMENT ON VIEW silver.v_associate_hours_by_pay_period IS
'ADP payroll hours per associate per pay period (authoritative as-paid hours, '
'from bronze.assoc_hours_by_pay_period; manual load for now). One row per '
'associate x department x pay period x pay date. Cadence varies by client: '
'AEP is weekly (Mon-Sun periods = ISO weeks); Alliant Energy is bi-weekly on '
'two one-day-offset cycles sharing a pay date. For client-level rollups use '
'silver.v_oso_payroll_hours_by_period (OSO) or group by client + pay_date. '
'Includes negative-hours correction rows (payroll reversals) - sum, do not '
'filter. hours_paid = regular + overtime + other (other = paid absence).';

COMMENT ON COLUMN silver.v_associate_hours_by_pay_period.applicant_id IS 'Portal applicant id - joins v_hires_unified / v_active_roster lineage. Never NULL in current data.';
COMMENT ON COLUMN silver.v_associate_hours_by_pay_period.department_number IS 'ADP home department code (text, matches department_reporting_map.department_number).';
COMMENT ON COLUMN silver.v_associate_hours_by_pay_period.client IS 'From department_reporting_map (NULL if the department is unmapped). Currently AEP + Alliant Energy only.';
COMMENT ON COLUMN silver.v_associate_hours_by_pay_period.offering IS 'Service line from the department map (currently all OSO).';
COMMENT ON COLUMN silver.v_associate_hours_by_pay_period.period_start IS 'Pay period beginning date.';
COMMENT ON COLUMN silver.v_associate_hours_by_pay_period.period_end IS 'Pay period end date. MAX() = data freshness (manual load; no fixed cadence yet).';
COMMENT ON COLUMN silver.v_associate_hours_by_pay_period.pay_date IS 'Check date. Alliant''s two bi-weekly cycles share pay dates - group on this for one row per client pay event.';
COMMENT ON COLUMN silver.v_associate_hours_by_pay_period.period_days IS 'Inclusive period length: 7 = weekly, 14 = bi-weekly; a few short year-end split periods exist.';
COMMENT ON COLUMN silver.v_associate_hours_by_pay_period.hours_regular IS 'Regular hours paid. May be negative on correction rows.';
COMMENT ON COLUMN silver.v_associate_hours_by_pay_period.hours_overtime IS 'Overtime hours paid.';
COMMENT ON COLUMN silver.v_associate_hours_by_pay_period.hours_other IS 'Other paid hours - paid absence (PTO/holiday; spikes on holiday weeks).';
COMMENT ON COLUMN silver.v_associate_hours_by_pay_period.hours_paid IS 'regular + overtime + other. The executive "hours" basis chosen 2026-07-20 (total paid).';
