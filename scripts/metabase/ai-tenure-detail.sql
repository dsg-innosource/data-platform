
/*
 * Metabase Model: AI Tenure Detail
 * URL: [Add Metabase URL here]
 * Description: Detailed tenure analysis with AI scoring across multiple passes for non-pipeline, pipeline, and rehire scenarios
 */

WITH adp_tenure AS (
    SELECT * FROM bronze.adp_tenure_history h
    WHERE h.snapshot_date = (
            SELECT MAX(h2.snapshot_date) FROM bronze.adp_tenure_history h2
        )
),

-- ADP base data with authoritative client info via department code
adp_base AS (
    SELECT
        a.applicant_id,
        a.adp_id,
        a.payroll_name,
        a.email,
        a.client AS adp_client_name, -- Keep original ADP client name for reference
        a.client_code,
        a.home_department_code,
        a.requisition_key AS adp_requisition_key,
        a.requisition_id AS adp_requisition_id,
        a.rehire_date,
        COALESCE(a.rehire_date, a.hire_date) AS calc_hire_date,
        COALESCE(a.termination_date, DATE(NOW())) AS calc_termination_date,
        CASE
            WHEN a.termination_date IS NULL THEN true
            ELSE false
        END AS is_active,
        COALESCE(a.termination_date, DATE(NOW()))::date - COALESCE(a.rehire_date, a.hire_date)::date AS tenure_days,
        -- Get authoritative client info via department
        pd.client_id AS auth_client_id,
        pc.name AS auth_client_name
    FROM adp_tenure a
    LEFT JOIN bronze.portal_departments pd ON a.home_department_code = pd.code
    LEFT JOIN bronze.portal_clients pc ON pd.client_id = pc.id
    WHERE 0 = 0
        AND NOT COALESCE(a.termination_reason, '') = 'Company Code Change'
        AND NOT a.client = 'INTERNAL'
),
-- Get requisition details and fill missing requisition_ids
requisition_details AS (
    SELECT
        ab.*,
        COALESCE(ab.adp_requisition_id, pr.id) AS final_requisition_id,
        COALESCE(ab.adp_requisition_key, pr.requisition_key) AS final_requisition_key,
        pr.use_jakib_ai,
        pr.is_pipeline,
        pr.fill_by,
        pc.name AS portal_client_name
    FROM adp_base ab
    LEFT JOIN bronze.portal_requisitions pr
        ON ab.adp_requisition_key = pr.requisition_key
    LEFT JOIN bronze.portal_clients pc ON pr.client_id = pc.id
),

-- All Jakib scores (shared across passes)
all_jakib_scores AS (
    SELECT
        j.applicant_id,
        j.score,
        j.requisition_id,
        j.scored_at
    FROM bronze.portal_jakib_results j
),

-- All resume scores (shared across passes)
all_resume_scores AS (
    SELECT
        rs.applicant_id,
        rs.resume_only_score,
        rs.requisition_id,
        rs.created_at
    FROM bronze.portal_resume_scores rs
),

-- PASS 1: Non-pipeline requisitions - direct req_id + applicant_id match (NO REHIRES)
pass1_results AS (
    SELECT
        rd.*,
        ajs.score AS jakib_score,
        ars.resume_only_score,
        'PASS1' AS pass_source
    FROM requisition_details rd
    LEFT JOIN all_jakib_scores ajs
        ON rd.applicant_id = ajs.applicant_id
        AND rd.final_requisition_id = ajs.requisition_id
    LEFT JOIN all_resume_scores ars
        ON rd.applicant_id = ars.applicant_id
        AND rd.final_requisition_id = ars.requisition_id
    WHERE rd.final_requisition_id IS NOT NULL
        AND COALESCE(rd.is_pipeline, false) = false
        AND rd.rehire_date IS NULL -- NO REHIRES in Pass 1
),

-- PASS 2: Pipeline requisitions - find closest requisition by fill_by date for same client (NO REHIRES)
pipeline_closest_reqs AS (
    SELECT
        rd.applicant_id,
        rd.auth_client_id,
        pr.id AS closest_req_id,
        pr.requisition_key AS closest_req_key,
        pr.fill_by,
        ROW_NUMBER() OVER (
            PARTITION BY rd.applicant_id
            ORDER BY ABS(pr.fill_by - rd.calc_hire_date)
        ) as rn
    FROM requisition_details rd
    INNER JOIN bronze.portal_requisitions pr ON pr.client_id = rd.auth_client_id
    WHERE rd.final_requisition_id IS NOT NULL
        AND COALESCE(rd.is_pipeline, false) = true
        AND rd.auth_client_id IS NOT NULL -- Ensure we have authoritative client
        AND pr.fill_by IS NOT NULL
        AND rd.rehire_date IS NULL -- NO REHIRES in Pass 2
),

pass2_results AS (
    SELECT
        rd.*,
        ajs.score AS jakib_score,
        ars.resume_only_score,
        pcr.closest_req_id AS pipeline_req_used,
        'PASS2' AS pass_source
    FROM requisition_details rd
    INNER JOIN pipeline_closest_reqs pcr
        ON rd.applicant_id = pcr.applicant_id
        AND pcr.rn = 1 -- Closest fill_by date
    LEFT JOIN all_jakib_scores ajs
        ON rd.applicant_id = ajs.applicant_id
        AND pcr.closest_req_id = ajs.requisition_id
    LEFT JOIN all_resume_scores ars
        ON rd.applicant_id = ars.applicant_id
        AND pcr.closest_req_id = ars.requisition_id
    WHERE rd.final_requisition_id IS NOT NULL
        AND COALESCE(rd.is_pipeline, false) = true
        AND rd.rehire_date IS NULL -- NO REHIRES in Pass 2
),

-- PASS 3: Rehires - find closest requisition by fill_by date for same client via authoritative client_id
rehire_closest_reqs AS (
    SELECT
        rd.applicant_id,
        rd.auth_client_id,
        pr.id AS closest_req_id,
        pr.requisition_key AS closest_req_key,
        pr.fill_by,
        ROW_NUMBER() OVER (
            PARTITION BY rd.applicant_id
            ORDER BY ABS(pr.fill_by - rd.calc_hire_date)
        ) as rn
    FROM requisition_details rd
    INNER JOIN bronze.portal_requisitions pr ON pr.client_id = rd.auth_client_id
    WHERE rd.rehire_date IS NOT NULL -- ONLY REHIRES in Pass 3
        AND rd.auth_client_id IS NOT NULL -- Ensure we have authoritative client
        AND pr.fill_by IS NOT NULL
),

pass3_results AS (
    SELECT
        rd.*,
        ajs.score AS jakib_score,
        ars.resume_only_score,
        rcr.closest_req_id AS rehire_req_used,
        'PASS3' AS pass_source
    FROM requisition_details rd
    INNER JOIN rehire_closest_reqs rcr
        ON rd.applicant_id = rcr.applicant_id
        AND rcr.rn = 1 -- Closest fill_by date
    LEFT JOIN all_jakib_scores ajs
        ON rd.applicant_id = ajs.applicant_id
        AND rcr.closest_req_id = ajs.requisition_id
    LEFT JOIN all_resume_scores ars
        ON rd.applicant_id = ars.applicant_id
        AND rcr.closest_req_id = ars.requisition_id
    WHERE rd.rehire_date IS NOT NULL -- ONLY REHIRES in Pass 3
),

-- Combine results from all passes
combined_results AS (
    SELECT
        applicant_id, adp_id, payroll_name, is_active,
        calc_hire_date, calc_termination_date, tenure_days,
        jakib_score, resume_only_score,
        final_requisition_id AS cal_requisition_id,
        final_requisition_key AS calc_requisition_key,
        auth_client_name AS calc_client, -- Use authoritative client name
        use_jakib_ai, pass_source
    FROM pass1_results

    UNION ALL

    SELECT
        applicant_id, adp_id, payroll_name, is_active,
        calc_hire_date, calc_termination_date, tenure_days,
        jakib_score, resume_only_score,
        pipeline_req_used AS cal_requisition_id,
        final_requisition_key AS calc_requisition_key,
        auth_client_name AS calc_client, -- Use authoritative client name
        use_jakib_ai, pass_source
    FROM pass2_results

    UNION ALL

    SELECT
        applicant_id, adp_id, payroll_name, is_active,
        calc_hire_date, calc_termination_date, tenure_days,
        jakib_score, resume_only_score,
        rehire_req_used AS cal_requisition_id,
        final_requisition_key AS calc_requisition_key,
        auth_client_name AS calc_client, -- Use authoritative client name
        use_jakib_ai, pass_source
    FROM pass3_results
),

-- Final results with AI categorization
final_data AS (
    SELECT DISTINCT
        cr.*,
        -- AI completion status
        CASE
            WHEN cr.jakib_score IS NOT NULL THEN true
            ELSE false
        END AS completed_bot,

        -- AI category logic
        CASE
            WHEN cr.jakib_score > 86 OR cr.resume_only_score > 81 THEN 'GREEN'
            WHEN cr.jakib_score IS NULL AND cr.resume_only_score > 81 THEN 'GREEN'
            WHEN cr.jakib_score > 67 OR cr.resume_only_score > 57 THEN 'YELLOW'
            WHEN cr.jakib_score IS NULL AND cr.resume_only_score > 57 THEN 'YELLOW'
            WHEN cr.jakib_score >= 0 OR cr.resume_only_score >= 0 THEN 'RED'
            WHEN cr.jakib_score IS NULL AND cr.resume_only_score >= 0 THEN 'RED'
            ELSE 'NONE'
        END AS ai_category
    FROM combined_results cr
)

SELECT
    applicant_id, adp_id, payroll_name, is_active,
    calc_hire_date, calc_termination_date, tenure_days,
    jakib_score, resume_only_score, cal_requisition_id,
    calc_requisition_key, calc_client, use_jakib_ai,
    completed_bot, ai_category
FROM final_data;
