-- ============================================================================
-- Silver Layer View: v_oso_roster_unified
-- Purpose: Unified agent roster across OSO clients (AEP, Alliant Energy) for
--          joins and agent-level drill-downs.
-- Created: 2026-06-04
-- Design:  docs/oso-silver-views-design.md
-- ============================================================================
-- Grain: one row per agent per client.
--
-- NOT the source for headcount KPIs — both rosters are current-state only.
-- Headcount / net gain come from silver.fact_active_headcount JOIN
-- silver.department_reporting_map (offering = 'OSO'), per the Contract
-- Staffing dashboard methodology (no view required; app-side filter).
--
-- Identifier notes:
--   Alliant — "Agent ID" is the NICE/CXone identifier (joins to
--   alliant_energy_daily_aht."Agent ID"); "Employee #" and "A0" are internal
--   Alliant identifiers. Names in bronze come in two shapes:
--   attendance = "First Last", daily_aht = "Last, First" — agent_id is the
--   reliable join key where available.
--   AEP — s_number joins to aep_oso_agent_stats."S_Number". Pulse_name is the
--   name as it appears in Pulse exports (joins to aep_oso_login.agent_name).
-- ============================================================================

CREATE OR REPLACE VIEW silver.v_oso_roster_unified AS

-- Alliant Energy: team roster
SELECT
    'Alliant Energy'::text          AS client_name,
    'alliant_energy'::text          AS source_system,
    r."Employee"                    AS agent_name,
    r."First Name"                  AS first_name,
    r."Last Name"                   AS last_name,
    r."Agent ID"::text              AS agent_id,        -- NICE/CXone ID
    r."Employee #"::text            AS employee_number, -- internal Alliant ID
    INITCAP(COALESCE(r."Employee Status", 'Unknown'))   AS status,
    (r."Employee Status" ILIKE 'active')                AS is_active,
    r."Team Leads"                  AS team_lead,
    r."Role"                        AS role,
    NULL::text                      AS location,
    r."Hire Date"::date             AS hire_date,
    r."Trainer"                     AS trainer,
    r.email                         AS email
FROM bronze.alliant_energy_team_roster r

UNION ALL

-- AEP: supervisor/roster file
SELECT
    'AEP'::text                     AS client_name,
    'aep_oso'::text                 AS source_system,
    s."Agents"                      AS agent_name,
    NULL::text                      AS first_name,
    NULL::text                      AS last_name,
    s.s_number                      AS agent_id,        -- joins agent_stats."S_Number"
    NULL::text                      AS employee_number,
    CASE WHEN s."Active" THEN 'Active' ELSE 'Inactive' END AS status,
    COALESCE(s."Active", FALSE)     AS is_active,
    s."Supervisor"                  AS team_lead,
    CASE WHEN s."Class" IS NOT NULL
         THEN 'Class ' || s."Class" END                 AS role,
    s."Location"                    AS location,
    NULL::date                      AS hire_date,
    NULL::text                      AS trainer,
    NULL::text                      AS email
FROM bronze.aep_oso_supervisor s;

-- ============================================================================
-- Documentation
-- ============================================================================

COMMENT ON VIEW silver.v_oso_roster_unified IS
'Unified OSO agent roster (AEP + Alliant Energy). One row per agent per '
'client; current-state only (no history). NOT the source for headcount KPIs — '
'use silver.fact_active_headcount JOIN silver.department_reporting_map '
'(offering = ''OSO'') for headcount and net gain. Use this view for agent '
'attributes (team lead, status, IDs) and as the name/ID bridge between the '
'telephony and hours views. Many columns are NULL for one side — both rosters '
'capture different attributes.';

COMMENT ON COLUMN silver.v_oso_roster_unified.client_name IS 'OSO client: ''AEP'' or ''Alliant Energy''.';
COMMENT ON COLUMN silver.v_oso_roster_unified.source_system IS 'Bronze source family: aep_oso (supervisor file) or alliant_energy (team roster).';
COMMENT ON COLUMN silver.v_oso_roster_unified.agent_name IS 'Display name from the source roster. Alliant = ''First Last''; AEP = name as in the supervisor file.';
COMMENT ON COLUMN silver.v_oso_roster_unified.first_name IS 'Alliant only; NULL for AEP.';
COMMENT ON COLUMN silver.v_oso_roster_unified.last_name IS 'Alliant only; NULL for AEP.';
COMMENT ON COLUMN silver.v_oso_roster_unified.agent_id IS 'Platform agent ID. Alliant = NICE/CXone ''Agent ID'' (joins alliant_energy_daily_aht."Agent ID"). AEP = s_number (joins aep_oso_agent_stats."S_Number").';
COMMENT ON COLUMN silver.v_oso_roster_unified.employee_number IS 'Internal Alliant ''Employee #''; NULL for AEP.';
COMMENT ON COLUMN silver.v_oso_roster_unified.status IS '''Active'' / ''Inactive'' (Alliant may also carry other statuses, INITCAP-normalized). Prefer is_active for filtering.';
COMMENT ON COLUMN silver.v_oso_roster_unified.is_active IS 'TRUE = currently active per the client roster. Roster-based; may drift from ADP — not authoritative for headcount.';
COMMENT ON COLUMN silver.v_oso_roster_unified.team_lead IS 'Team lead (Alliant ''Team Leads'') or supervisor (AEP).';
COMMENT ON COLUMN silver.v_oso_roster_unified.role IS 'Alliant role (CSR/Senior/etc.). AEP = ''Class N'' training class when present.';
COMMENT ON COLUMN silver.v_oso_roster_unified.location IS 'AEP only; NULL for Alliant.';
COMMENT ON COLUMN silver.v_oso_roster_unified.hire_date IS 'Alliant only; NULL for AEP. For tenure analysis prefer ADP sources.';
COMMENT ON COLUMN silver.v_oso_roster_unified.trainer IS 'Alliant only.';
COMMENT ON COLUMN silver.v_oso_roster_unified.email IS 'Alliant only.';
