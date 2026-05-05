-- ============================================================================
-- silver.v_sales_users
-- ----------------------------------------------------------------------------
-- Purpose : Salesforce user roster, surfaced for reporting joins
-- Grain   : One row per SF user
-- Notes   : Many user fields (Title, ManagerId, Department, Division) are
--           sparsely populated. is_sales_user = true filters out system
--           accounts and inactive users, which is the right filter for
--           rep-level rollups in nearly all reports.
-- ============================================================================

DROP VIEW IF EXISTS silver.v_sales_users;

CREATE VIEW silver.v_sales_users AS
SELECT
    u."Id"                          AS user_id,
    u."Name"                        AS user_name,
    u."FirstName"                   AS first_name,
    u."LastName"                    AS last_name,
    u."Email"                       AS email,
    u."Alias"                       AS alias,
    u."Title"                       AS title,
    u."Department"                  AS department,
    u."Division"                    AS division,
    u."ManagerId"                   AS manager_user_id,
    mgr."Name"                      AS manager_name,
    u."UserRoleId"                  AS user_role_id,
    u."UserType"                    AS user_type,
    u."IsActive"                    AS is_active,
    u."CreatedDate"                 AS created_at,
    u."LastLoginDate"               AS last_login_at,
    -- "is this person a sales rep we should include in rollups?"
    CASE
        WHEN u."UserType" <> 'Standard'                 THEN false
        WHEN u."IsActive" = false                       THEN false
        WHEN u."Name" ILIKE '%integration%'             THEN false
        WHEN u."Name" ILIKE '%automated process%'       THEN false
        WHEN u."Name" ILIKE '%security user%'           THEN false
        WHEN u."Name" ILIKE '%insights%'                THEN false
        ELSE true
    END                             AS is_sales_user
FROM bronze."sf_User" u
LEFT JOIN bronze."sf_User" mgr
       ON mgr."Id" = u."ManagerId";

COMMENT ON VIEW silver.v_sales_users IS
'Salesforce user roster with manager enrichment. Filter is_sales_user = true for rep-level rollups.';
