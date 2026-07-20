-- ============================================================================
-- Silver Layer View: v_oso_payroll_hours_by_period
-- Purpose: Client x pay-date rollup of ADP payroll hours feeding the OSO
--          dashboard's per-client hours tiles/trends at NATIVE pay-period
--          grain (decision 2026-07-20: actual paid hours, no weekly
--          allocation/approximation).
-- Created: 2026-07-20
-- Design:  docs/oso-silver-views-design.md (hours addendum)
--
-- DEPENDS ON (deploy first):
--   silver.v_associate_hours_by_pay_period
-- ============================================================================
-- Grain: one row per client per pay date.
--
-- Grouping by pay_date (not period_start) is what makes Alliant clean: its two
-- bi-weekly cycles are offset by one day but share the check date, so one row
-- covers both; period_start/period_end span the union (15 inclusive days).
-- AEP pay periods are Mon-Sun weeks, so its rows ARE ISO weeks (7 days).
--
-- Rows only exist once payroll has processed - there is no "partial period"
-- state to guard against (unlike the telephony feeds).
-- ============================================================================

CREATE OR REPLACE VIEW silver.v_oso_payroll_hours_by_period AS

SELECT
    v.client,
    v.pay_date,
    MIN(v.period_start)                              AS period_start,
    MAX(v.period_end)                                AS period_end,
    (MAX(v.period_end) - MIN(v.period_start) + 1)    AS period_days,
    CASE
        WHEN MAX(v.period_end) - MIN(v.period_start) + 1 <= 7 THEN 'weekly'
        ELSE 'biweekly'
    END                                              AS cadence,
    ROUND(SUM(v.hours_regular),  2)                  AS hours_regular,
    ROUND(SUM(v.hours_overtime), 2)                  AS hours_overtime,
    ROUND(SUM(v.hours_other),    2)                  AS hours_other,
    ROUND(SUM(v.hours_paid),     2)                  AS hours_paid,
    COUNT(DISTINCT v.applicant_id)
        FILTER (WHERE v.hours_paid > 0)              AS associates_paid
FROM silver.v_associate_hours_by_pay_period v
WHERE v.offering = 'OSO'
GROUP BY v.client, v.pay_date;

-- ============================================================================
-- Documentation
-- ============================================================================

COMMENT ON VIEW silver.v_oso_payroll_hours_by_period IS
'ADP payroll hours per OSO client per pay date - the authoritative "hours" '
'source for the OSO dashboard, at native pay-period grain (no weekly '
'allocation). AEP rows are weekly Mon-Sun periods (= ISO weeks, ~3.7-4.3k '
'hours_paid); Alliant Energy rows are bi-weekly (two one-day-offset cycles '
'merged on the shared pay date, 15-day span, ~14k hours_paid). Cadences '
'differ, so DO NOT sum across clients for a "program hours" number without '
'normalizing spans. Rows appear only after payroll processes (no partials); '
'freshness = MAX(period_end) per client. Underlying feed is a manual load.';

COMMENT ON COLUMN silver.v_oso_payroll_hours_by_period.client IS 'OSO client: ''AEP'' or ''Alliant Energy''. Matches department_reporting_map.client.';
COMMENT ON COLUMN silver.v_oso_payroll_hours_by_period.pay_date IS 'Check date (grain, with client). Fridays; AEP weekly, Alliant every other Friday.';
COMMENT ON COLUMN silver.v_oso_payroll_hours_by_period.period_start IS 'Earliest period-beginning across merged cycles.';
COMMENT ON COLUMN silver.v_oso_payroll_hours_by_period.period_end IS 'Latest period-end across merged cycles. MAX() per client = hours freshness.';
COMMENT ON COLUMN silver.v_oso_payroll_hours_by_period.period_days IS 'Inclusive span: 7 for AEP weeks, 15 for merged Alliant bi-weeklies; short year-end splits exist.';
COMMENT ON COLUMN silver.v_oso_payroll_hours_by_period.cadence IS '''weekly'' (span <= 7 days) or ''biweekly''. Label periods, do not compare across cadences.';
COMMENT ON COLUMN silver.v_oso_payroll_hours_by_period.hours_regular IS 'Regular hours paid in the period (net of correction rows).';
COMMENT ON COLUMN silver.v_oso_payroll_hours_by_period.hours_overtime IS 'Overtime hours paid in the period.';
COMMENT ON COLUMN silver.v_oso_payroll_hours_by_period.hours_other IS 'Paid absence (PTO/holiday) - spikes on holiday weeks as regular dips.';
COMMENT ON COLUMN silver.v_oso_payroll_hours_by_period.hours_paid IS 'regular + overtime + other. The dashboard headline hours basis (total paid, decision 2026-07-20).';
COMMENT ON COLUMN silver.v_oso_payroll_hours_by_period.associates_paid IS 'Distinct associates with hours_paid > 0 in the period.';
