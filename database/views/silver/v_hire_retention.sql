-- --------------------------------------
-- silver.v_hire_retention
-- --------------------------------------
/*
  Purpose : Checkpoint retention surface. One row per hire (start), with that hire's
            earliest qualifying termination matched in, so consumers can answer
            "of associates who reached the N-week mark, how many are still active?"
            without writing cohort/survival logic each time.

  Grain   : One row per hire/start event (v_hires_unified.event_id). Rehires produce
            multiple rows; each start is matched to the earliest termination on/after it.

  Source  : silver.v_hires_unified         (start spine — actual_start_date, offering, dept)
            silver.v_terminations_unified   (earliest term on/after start; is_excluded = false)

  Scope   : Bounded to starts in the trailing 365 days — covers the 6/8/15-week
            checkpoints with wide margin and keeps the view fast. Widen the interval
            if longer-horizon retention is needed.

  Usage   : Filter is_offering_excluded = false for every retention KPI.
            Scope a service line via offering (e.g. 'Contract').
            Retention @ N weeks =
              count(active_at_Nw) filter (reached_Nw and not is_offering_excluded)
              / count(*)          filter (reached_Nw and not is_offering_excluded)
            is_currently_active powers the Active column in retention-by-client.

  Checkpoints: 6w = 42d, 8w = 56d, 15w = 105d.

  Created : 2026-06-03
*/
DROP VIEW IF EXISTS silver.v_hire_retention;
CREATE OR REPLACE VIEW silver.v_hire_retention AS
WITH hires AS (
  SELECT h.source_system, h.event_id AS hire_event_id, h.applicant_id, h.full_name,
         h.actual_start_date, h.start_iso_year, h.start_iso_week,
         h.client_id, h.client_name, h.recruiter_id, h.recruiter_name,
         h.department_code, h.department_id, h.department_name,
         h.offering, h.service, h.is_offering_excluded, h.is_pipeline
  FROM silver.v_hires_unified h
  WHERE h.actual_start_date IS NOT NULL
    AND h.actual_start_date >= (CURRENT_DATE - INTERVAL '365 days')::date
),
term_match AS (                          -- earliest termination on/after each start
  SELECT hr.hire_event_id, MIN(t.termination_date) AS termination_date
  FROM hires hr
  JOIN silver.v_terminations_unified t
    ON t.applicant_id = hr.applicant_id
   AND t.is_excluded = FALSE
   AND t.termination_date >= hr.actual_start_date
  GROUP BY hr.hire_event_id
)
SELECT hr.*, tm.termination_date,
  (tm.termination_date - hr.actual_start_date)                                    AS days_to_term,
  (CURRENT_DATE - hr.actual_start_date)                                           AS days_since_start,
  ((CURRENT_DATE - hr.actual_start_date) >= 42)                                   AS reached_6w,
  ((CURRENT_DATE - hr.actual_start_date) >= 56)                                   AS reached_8w,
  ((CURRENT_DATE - hr.actual_start_date) >= 105)                                  AS reached_15w,
  (tm.termination_date IS NULL OR (tm.termination_date - hr.actual_start_date) > 42)  AS active_at_6w,
  (tm.termination_date IS NULL OR (tm.termination_date - hr.actual_start_date) > 56)  AS active_at_8w,
  (tm.termination_date IS NULL OR (tm.termination_date - hr.actual_start_date) > 105) AS active_at_15w,
  (tm.termination_date IS NULL OR tm.termination_date > CURRENT_DATE)             AS is_currently_active
FROM hires hr
LEFT JOIN term_match tm USING (hire_event_id);

COMMENT ON VIEW silver.v_hire_retention IS
  'One row per hire (start) from v_hires_unified, bounded to starts in the trailing 365 days, '
  'with that hire''s earliest termination on/after the start date matched from v_terminations_unified '
  '(applicant_id, is_excluded = false). Powers cohort-free retention: of associates who reached an '
  'N-week checkpoint, how many are still active at that mark. Filter is_offering_excluded = false for '
  'every retention KPI; scope to a service line with offering (e.g. ''Contract''). Rehires are evaluated '
  'per start row against the earliest qualifying termination, so a rehired associate contributes multiple rows.';

COMMENT ON COLUMN silver.v_hire_retention.hire_event_id IS 'Grain. v_hires_unified.event_id — one row per hire/start event.';
COMMENT ON COLUMN silver.v_hire_retention.termination_date IS 'Earliest termination on/after actual_start_date for this applicant (is_excluded = false); NULL if still active.';
COMMENT ON COLUMN silver.v_hire_retention.days_to_term IS 'termination_date - actual_start_date (days); NULL if no qualifying termination.';
COMMENT ON COLUMN silver.v_hire_retention.days_since_start IS 'CURRENT_DATE - actual_start_date (days).';
COMMENT ON COLUMN silver.v_hire_retention.reached_6w IS 'TRUE once ≥42 days have elapsed since start — the 6-week checkpoint is measurable. Use as the retention denominator filter.';
COMMENT ON COLUMN silver.v_hire_retention.reached_8w IS 'TRUE once ≥56 days have elapsed since start (8-week checkpoint measurable).';
COMMENT ON COLUMN silver.v_hire_retention.reached_15w IS 'TRUE once ≥105 days have elapsed since start (15-week checkpoint measurable).';
COMMENT ON COLUMN silver.v_hire_retention.active_at_6w IS 'Survived to the 6-week mark: no termination, or terminated >42 days after start. Only meaningful where reached_6w.';
COMMENT ON COLUMN silver.v_hire_retention.active_at_8w IS 'Survived to the 8-week mark: no termination, or terminated >56 days after start. Only meaningful where reached_8w.';
COMMENT ON COLUMN silver.v_hire_retention.active_at_15w IS 'Survived to the 15-week mark: no termination, or terminated >105 days after start. Only meaningful where reached_15w.';
COMMENT ON COLUMN silver.v_hire_retention.is_currently_active IS 'No termination as of CURRENT_DATE (or termination future-dated). Powers the Active column in retention-by-client.';