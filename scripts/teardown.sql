-- =============================================================================
-- FRAUD DETECTION DEMO: Teardown / Cleanup
-- =============================================================================
-- Run this script to remove ALL demo objects from the account.
-- WARNING: This is destructive and irreversible. All data will be lost.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- Drop compute pool (must drop services first if any are running)
-- ALTER COMPUTE POOL FRAUD_DEMO_CPU_POOL STOP ALL;
DROP COMPUTE POOL IF EXISTS FRAUD_DEMO_CPU_POOL;

-- Drop warehouses
DROP WAREHOUSE IF EXISTS FRAUD_DEMO_LOAD_WH;
DROP WAREHOUSE IF EXISTS FRAUD_DEMO_WH;
DROP WAREHOUSE IF EXISTS FRAUD_DEMO_TRAIN_WH;

-- Drop databases (cascades all schemas, tables, DTs, views, stages)
DROP DATABASE IF EXISTS FRAUD_DEMO_DEV;
DROP DATABASE IF EXISTS FRAUD_DEMO_STAGING;
DROP DATABASE IF EXISTS FRAUD_DEMO_PROD;

-- Drop roles
DROP ROLE IF EXISTS FRAUD_DS_DEV;
DROP ROLE IF EXISTS FRAUD_MLOPS;

-- =============================================================================
-- CLEANUP COMPLETE
-- =============================================================================
