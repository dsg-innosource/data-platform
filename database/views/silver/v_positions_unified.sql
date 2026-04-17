-- ============================================================================
-- Silver Layer View: v_positions_unified v3 (3NF REFACTOR)
-- Purpose: Present Portal 1.0 requisitions in Portal 2.0 POSITION structure
-- Updated: 2025-02-25
-- ============================================================================
-- 3NF DESIGN NOTES:
--   A "position" is the JOB TEMPLATE — who, what, where, and how to screen.
--   It does NOT include fulfillment details (openings, dates, urgency).
--   Those belong on v_orders_unified (the specific request to fill the position).
--
-- Portal 1 Mapping:
--   portal_requisitions (1 row) → 1 position + 1 order (1:1 in P1)
--
-- Portal 2 Mapping (future):
--   positions (1 row) → many orders (1:many)
--   Join key: (source_system, position_id)
-- ============================================================================

DROP VIEW IF EXISTS silver.v_positions_unified;

CREATE OR REPLACE VIEW silver.v_positions_unified AS
SELECT

    -- -------------------------------------------------------------------------
    -- Identity & Source Tracking
    -- -------------------------------------------------------------------------
    r.id                            AS position_id,     -- Native numeric ID; fast join key
    'portal_v1'                     AS source_system,   -- Differentiates P1 vs P2 when P2 lands

    -- -------------------------------------------------------------------------
    -- Position / Job Details
    -- -------------------------------------------------------------------------
    r.position                      AS position_title,
    r.pay_rate,

    -- -------------------------------------------------------------------------
    -- Client & Geography  (position-level: WHERE is this job?)
    -- -------------------------------------------------------------------------
    r.client_id,
    c.name                          AS client_name,
    reg.id                          AS region_id,
    reg.name                        AS region_name,
    reg.city                        AS region_city,
    reg.state                       AS region_state,

    -- -------------------------------------------------------------------------
    -- Business Classification
    -- -------------------------------------------------------------------------
    dlob.id                         AS line_of_business_id,
    dlob.name                       AS line_of_business,

    -- -------------------------------------------------------------------------
    -- AI Configuration  (position-level: HOW should screening work?)
    -- -------------------------------------------------------------------------
    r.use_jakib_ai,
    r.use_voice_call_ai,
    r.ai_context,
    r.ai_scoring_context,
    r.agent_type,

    -- -------------------------------------------------------------------------
    -- Recruiter Assignment  (position-level: WHO owns this position?)
    -- -------------------------------------------------------------------------
    r.primary_recruiter_id          AS recruiter_id,
    CASE
        WHEN r.primary_recruiter_id IS NOT NULL
        THEN u.first_name || ' ' || u.last_name
    END                             AS recruiter_name,
    u.email                         AS recruiter_email,

    -- -------------------------------------------------------------------------
    -- Position Status
    -- (In P1, is_closed lives on the requisition = position level.
    --  In P2, position has its own position_status_id.
    --  We surface both a raw flag and a derived label for compatibility.)
    -- -------------------------------------------------------------------------
    r.is_closed,
    r.closed_at,
    CASE
        WHEN r.is_closed = TRUE THEN 'CLOSED'
        ELSE 'OPEN'
    END                             AS position_status,

    -- -------------------------------------------------------------------------
    -- Capacity Template  (class_size is a position-level template in P2)
    -- (number_of_openings for a specific request lives on the order)
    -- -------------------------------------------------------------------------
    r.class_size,

    -- -------------------------------------------------------------------------
    -- Legacy / Cross-Reference
    -- -------------------------------------------------------------------------
    r.requisition_key,              -- Keep for ADP joins (adp_tenure_history.requisition_key)
    r.is_pipeline, -- needed short term for reporting
    -- -------------------------------------------------------------------------
    -- Audit
    -- -------------------------------------------------------------------------
    r.created_at,
    r.updated_at,
    r.deleted_at

FROM bronze.portal_requisitions r
LEFT JOIN bronze.portal_clients c
    ON r.client_id = c.id
LEFT JOIN bronze.portal_regions reg
    ON c.region_id = reg.id
LEFT JOIN bronze.portal_dashboard_lines_of_business dlob
    ON r.requisition_type_id = dlob.id
LEFT JOIN bronze.portal_users u
    ON r.primary_recruiter_id = u.id

WHERE r.deleted_at IS NULL;


COMMENT ON VIEW silver.v_positions_unified IS
'Unified position view (3NF). Represents the JOB TEMPLATE — title, pay, client,
AI config, recruiter assignment, and business classification.
Does NOT include fulfillment details (openings, dates, urgency) — see v_orders_unified.

Portal 1: 1 requisition = 1 position (1:1 with orders).
Portal 2: 1 position = many orders (1:many).

Join key: (source_system, position_id).
ADP join: requisition_key → adp_tenure_history.requisition_key.';
