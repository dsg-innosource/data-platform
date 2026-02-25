-- ============================================================================
-- Silver Layer View: v_positions_unified (OPTIMIZED VERSION)
-- Purpose: Present Portal 1.0 requisitions in Portal 2.0 position structure
-- Created: 2025-02-17
-- ============================================================================
-- PERFORMANCE OPTIMIZATION: Uses original numeric IDs for fast joins
-- Differentiation via source_system column instead of string prefixes
-- ============================================================================

drop view if exists silver.v_positions_unified;

CREATE OR REPLACE VIEW silver.v_positions_unified AS
SELECT 
    -- Identity & Source Tracking (OPTIMIZED: no string concatenation)
    r.id AS position_id,  -- Original numeric ID for fast joins
    'portal_v1' AS source_system,
    -- r.requisition_key,
    concat(c.name,' - ', r.open_date, ' - ', r.position, ' - ', r.fill_by) as order_key,
    
    -- Position Details
    r.position AS position_title,
    r.pay_rate,
    
    -- Client Relationship
    r.client_id,
    c.name AS client_name,
    reg.name AS region_name,
    reg.city AS region_city,
    reg.state AS region_state,
    
    -- Business Classification
    dlob.name AS line_of_business,
    
    -- Dates & Status
    r.open_date,
    r.fill_by AS fill_by_date,
    r.is_closed,
    r.closed_at,
    
    -- Capacity
    r.number_of_openings,
    r.class_size,
    
    -- Urgency Calculation
    CASE 
        WHEN r.is_closed = TRUE THEN 'CLOSED'
        WHEN r.fill_by::date < CURRENT_DATE THEN 'OVERDUE'
        WHEN r.fill_by::date <= CURRENT_DATE + INTERVAL '7 days' THEN 'DUE_SOON'
        ELSE 'ON_TRACK'
    END AS urgency_status,
    
    (r.fill_by::date - CURRENT_DATE) AS days_remaining,
    
    -- AI Configuration
    r.use_jakib_ai,
    r.use_voice_call_ai,
    r.ai_context,
    r.agent_type,
    
    -- Recruiter Assignment
    r.primary_recruiter_id AS recruiter_id,
    CASE 
        WHEN r.primary_recruiter_id IS NOT NULL 
        THEN u.first_name || ' ' || u.last_name 
    END AS recruiter_name,
    u.email AS recruiter_email,
    
    -- Audit
    r.created_at,
    r.updated_at,
    r.deleted_at
    
FROM bronze.portal_requisitions r
LEFT JOIN bronze.portal_clients c ON r.client_id = c.id
LEFT JOIN bronze.portal_regions reg ON c.region_id = reg.id
LEFT JOIN bronze.portal_dashboard_lines_of_business dlob 
    ON r.requisition_type_id = dlob.id
LEFT JOIN bronze.portal_users u ON r.primary_recruiter_id = u.id

WHERE r.deleted_at IS NULL
    -- AND NOT (r.deleted_at = TRUE OR r.is_pipeline = TRUE)
;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_positions_unified_source 
    ON silver.v_positions_unified_materialized (source_system, position_id);
    
CREATE INDEX IF NOT EXISTS idx_positions_unified_recruiter 
    ON silver.v_positions_unified_materialized (recruiter_id) 
    WHERE recruiter_id IS NOT NULL;

COMMENT ON VIEW silver.v_positions_unified IS 
'Unified view of positions. Uses original numeric IDs for fast joins. 
Differentiate portal versions via source_system column.
When Portal 2.0 added: will have ID collisions requiring composite keys (source_system, position_id).';
