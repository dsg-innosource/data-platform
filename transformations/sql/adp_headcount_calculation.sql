-- ADP Headcount Calculation
-- Calculates daily active headcount by department and inserts into silver.fact_active_headcount
-- 
-- Parameters:
--   :snapshot_date - The date of the data snapshot (YYYY-MM-DD)
--   :report_date - The reporting period date (YYYY-MM-DD)
--   :bronze_table - Source table name (default: bronze.adp_tenure_history)
--   :silver_table - Target table name (default: silver.fact_active_headcount)
--   :excluded_client_codes - Array of client codes to exclude (default: ['01100'])

WITH daily_active AS (
    SELECT 
        COUNT(DISTINCT adp_id) as active_count,
        home_department_code, 
        home_department_description
    FROM bronze.adp_tenure_history a
    WHERE a.position_status = 'Active'
        AND a.file_number IS NOT NULL
        AND a.client_code NOT IN ('01100')  -- Configurable via excluded_client_codes
        AND COALESCE(a.rehire_date, a.hire_date) < a.snapshot_date
        AND a.snapshot_date = :snapshot_date
    GROUP BY home_department_code, home_department_description
)
INSERT INTO silver.fact_active_headcount (
    department_number, 
    snapshot_date, 
    report_date, 
    active_count, 
    created_at
)
SELECT
    a.home_department_code,
    :snapshot_date as snapshot_date,
    :report_date as report_date,
    a.active_count,
    NOW()
FROM daily_active a
WHERE a.home_department_code IS NOT NULL;