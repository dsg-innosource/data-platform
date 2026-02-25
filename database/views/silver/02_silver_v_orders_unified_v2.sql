-- ============================================================================
-- Silver Layer View: v_orders_unified (OPTIMIZED VERSION)
-- Purpose: Present Portal 1.0 requisitions as orders in Portal 2.0 structure
-- Created: 2025-02-17
-- ============================================================================
-- PERFORMANCE OPTIMIZATION: Uses original numeric IDs for fast joins
-- In Portal 1: Each requisition IS an order (1:1 relationship)
-- In Portal 2: Multiple orders can exist per position (1:many relationship)
-- ============================================================================

CREATE OR REPLACE VIEW silver.v_orders_unified AS
SELECT 
    -- Identity & Source Tracking (OPTIMIZED)
    r.id AS order_id,  -- Original numeric ID for fast joins
    'portal_v1' AS source_system,
    
    -- Position Relationship (OPTIMIZED: numeric for joins)
    r.id AS position_id,  -- In P1, requisition = position = order
    r.requisition_key,
    r.position AS position_title,
    
    -- Client Context
    r.client_id,
    c.name AS client_name,
    
    -- Order Specifics
    r.number_of_openings,
    r.class_size,
    r.open_date,
    r.fill_by AS fill_by_date,
    
    -- Status
    r.is_closed,
    r.closed_at,
    
    -- Urgency (calculated)
    CASE 
        WHEN r.is_closed = TRUE THEN 'CLOSED'
        WHEN r.fill_by::date < CURRENT_DATE THEN 'OVERDUE'
        WHEN r.fill_by::date <= CURRENT_DATE + INTERVAL '7 days' THEN 'DUE_SOON'
        ELSE 'ON_TRACK'
    END AS urgency_status,
    
    (r.fill_by::date - CURRENT_DATE) AS days_remaining,
    
    -- Recruiter (from position/requisition)
    r.primary_recruiter_id AS recruiter_id,
    CASE 
        WHEN r.primary_recruiter_id IS NOT NULL 
        THEN u.first_name || ' ' || u.last_name 
    END AS recruiter_name,
    
    -- Audit
    r.created_at,
    r.updated_at
    
FROM bronze.portal_requisitions r
LEFT JOIN bronze.portal_clients c ON r.client_id = c.id
LEFT JOIN bronze.portal_users u ON r.primary_recruiter_id = u.id

WHERE r.deleted_at IS NULL
    -- AND NOT (r.is_deleted = TRUE OR r.is_pipeline = TRUE)
;

COMMENT ON VIEW silver.v_orders_unified IS 
'Unified view of orders. Uses native numeric IDs for performance.
In Portal 1: order_id = position_id (1:1 relationship).
In Portal 2: order_id separate from position_id (1:many relationship).
Differentiate via source_system column.';
