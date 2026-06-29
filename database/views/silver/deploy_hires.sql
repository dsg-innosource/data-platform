-- ============================================================================
-- deploy_hires.sql  —  ordered (re)deploy of the hires view + its dependent
-- ----------------------------------------------------------------------------
-- silver.v_hire_retention is built on top of silver.v_hires_unified, so the
-- hires view cannot be dropped/recreated while retention exists:
--   "cannot drop view silver.v_hires_unified because other objects depend on it"
-- This script drops the dependent FIRST, rebuilds the hires view, then rebuilds
-- the dependent. Run this instead of executing v_hires_unified.sql directly.
--
-- USAGE (psql, from THIS directory):
--     psql "$DATABASE_URL" -f deploy_hires.sql
--
-- NOT wrapped in BEGIN/COMMIT on purpose: v_hires_unified.sql runs
-- CREATE INDEX CONCURRENTLY (idx_adp_tenure_applicant_snap), which cannot run
-- inside a transaction block. \set ON_ERROR_STOP still aborts on the first
-- error, and the index is idempotent (IF NOT EXISTS) so re-runs are cheap.
--
-- Dependency (verified via pg_depend, 2026-06-29):
--     v_hires_unified  ->  v_hire_retention
--   v_hire_retention is a leaf — nothing depends on it or on v_hires_unified
--   besides this one view, so this two-step rebuild is complete.
-- ============================================================================

\set ON_ERROR_STOP on

-- 1) Drop the dependent first (reverse dependency order) ----------------------
DROP VIEW IF EXISTS silver.v_hire_retention;

-- 2) Rebuild in forward dependency order --------------------------------------
--    Each file self-DROPs its own view; the drop is harmless (no remaining
--    dependents) now that v_hire_retention is gone.
\i v_hires_unified.sql
\i v_hire_retention.sql

-- ============================================================================
-- Post-deploy sanity (optional — run manually):
--   SELECT hire_confirmation_status, COUNT(*) FROM silver.v_hires_unified GROUP BY 1;
--   SELECT COUNT(*) FROM silver.v_hire_retention;
-- ============================================================================
