"""Parameterized SQL for the recruiter-activity report.

The two main queries take :window_start and :window_end as bind parameters.
The weekend probe runs only on Monday and counts Saturday + Sunday rows.

Spec: routines/recruiter-activity.md § 4.
"""

# ── Section 02 totals ────────────────────────────────────────────────────────
# Spec: routines/recruiter-activity.md § 4.1
SECTION_02_TOTALS_SQL = """\
SELECT
    COUNT(*) FILTER (WHERE event_category = 'PHONE_SCREEN')     AS phone_screens,
    COUNT(*) FILTER (WHERE event_category = 'INNO_INTERVIEW')   AS inno_interviews,
    COUNT(*) FILTER (WHERE event_category = 'CLIENT_INTERVIEW') AS client_interviews,
    COUNT(*) FILTER (WHERE event_category = 'OFFER')            AS offers_extended,
    COUNT(*) FILTER (WHERE event_category = 'OFFER_ACCEPTED')   AS offers_accepted,
    COUNT(*) FILTER (WHERE event_category = 'HIRED')            AS hires
FROM silver.v_applicant_activity_events
WHERE event_date BETWEEN %(window_start)s AND %(window_end)s;
"""


# ── Section 03 recruiter breakdown ───────────────────────────────────────────
# Spec: routines/recruiter-activity.md § 4.2
SECTION_03_RECRUITERS_SQL = """\
SELECT
    recruiter_name,
    COUNT(*) FILTER (WHERE event_category = 'PHONE_SCREEN')   AS screens,
    COUNT(*) FILTER (WHERE event_category = 'INNO_INTERVIEW') AS inno_interviews,
    COUNT(*) FILTER (WHERE event_category = 'OFFER')          AS offers_extended,
    COUNT(*) FILTER (WHERE event_category = 'OFFER_ACCEPTED') AS offers_accepted
FROM silver.v_applicant_activity_events
WHERE event_date BETWEEN %(window_start)s AND %(window_end)s
  AND recruiter_id IS NOT NULL
GROUP BY recruiter_id, recruiter_name
ORDER BY offers_accepted DESC, screens DESC;
"""


# ── Weekend probe (Monday only) ──────────────────────────────────────────────
# Spec: routines/recruiter-activity.md § 4.3
WEEKEND_PROBE_SQL = """\
SELECT COUNT(*) AS weekend_count
FROM silver.v_applicant_activity_events
WHERE event_date BETWEEN %(window_start)s + INTERVAL '1 day' AND %(window_end)s;
"""
