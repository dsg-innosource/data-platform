-- ============================================================================
-- deploy.sql  —  silver sales views, ordered (re)deploy
-- ----------------------------------------------------------------------------
-- Drops every view in REVERSE dependency order, then recreates in FORWARD
-- order, inside one transaction. Run this instead of executing the view files
-- ad hoc — it avoids the "cannot drop view X because other objects depend on
-- it" errors caused by inter-view dependencies.
--
-- USAGE (psql, from THIS directory):
--     psql "$DATABASE_URL" -f deploy.sql
--
-- Uses psql \i includes, so run it with psql (not a GUI SQL console). If your
-- client doesn't support \i, run the files manually in the order listed below.
--
-- ASSUMES already present (unchanged, not redeployed here):
--     silver.v_sales_users, silver.v_sales_accounts,
--     silver.v_sales_activities, silver.v_sales_activity_daily,
--     silver.v_sales_meetings
--
-- Dependency chains handled:
--     v_sales_opportunities  -> v_sales_leads, v_sales_pipeline_summary
--     v_sales_campaign_members -> v_sales_campaigns
-- ============================================================================

\set ON_ERROR_STOP on

BEGIN;

-- 1) Drop dependents first (reverse dependency order) -------------------------
DROP VIEW IF EXISTS silver.v_sales_pipeline_summary;
DROP VIEW IF EXISTS silver.v_sales_campaigns;
DROP VIEW IF EXISTS silver.v_sales_leads;
DROP VIEW IF EXISTS silver.v_sales_campaign_members;
DROP VIEW IF EXISTS silver.v_sales_opportunities;

-- 2) Recreate in forward dependency order -------------------------------------
--    (each file also self-DROPs its own view; harmless after the drops above)
\i v_sales_opportunities.sql
\i v_sales_leads.sql
\i v_sales_pipeline_summary.sql
\i v_sales_campaign_members.sql
\i v_sales_campaigns.sql

COMMIT;

-- ============================================================================
-- Post-deploy sanity (optional — run manually):
--   SELECT table_name FROM information_schema.views
--   WHERE table_schema='silver' AND table_name LIKE 'v_sales_%' ORDER BY 1;
-- ============================================================================
