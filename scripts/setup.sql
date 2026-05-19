-- =============================================================================
-- FRAUD DETECTION DEMO: Infrastructure Setup
-- =============================================================================
-- This script creates all Snowflake objects required for the fraud detection demo.
-- Run once before executing any notebooks.
--
-- DESIGN CHOICES:
--   - Standalone infrastructure (no dependencies on other demos)
--   - 3 environments (DEV/STAGING/PROD) for realistic promotion workflow
--   - Right-sized warehouses per workload type (see comments per warehouse)
--   - Snowpark-optimized warehouse for ML training (256GB dedicated RAM)
--   - CPU_X64_XS compute pool for model serving (right-sized for XGBoost)
--
-- COST ESTIMATE (production-like, annual):
--   DT refresh (SMALL WH):        ~$3,343/yr
--   SPCS endpoint (CPU_X64_XS):   ~$2,409/yr
--   Training (monthly retrain):   ~$27/yr
--   TOTAL:                        ~$5,780/yr
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- SECTION 1: DATABASES
-- =============================================================================
-- Three environments mirror a real production workflow:
--   DEV: data scientists experiment freely
--   STAGING: validated models awaiting approval
--   PROD: live scoring endpoint + monitoring

CREATE DATABASE IF NOT EXISTS FRAUD_DEMO_DEV;
CREATE DATABASE IF NOT EXISTS FRAUD_DEMO_STAGING;
CREATE DATABASE IF NOT EXISTS FRAUD_DEMO_PROD;

-- =============================================================================
-- SECTION 2: SCHEMAS
-- =============================================================================
-- Schema-per-concern pattern for clear ownership and access control:
--   TRANSACTIONS: raw event data (source of truth)
--   FEATURES: Dynamic Tables + Feature Views (feature store)
--   ML: trained models, training sets, experiment artifacts
--   SERVING: SPCS endpoints and service objects
--   MONITORING: inference logs, model monitors, alerts

-- DEV schemas
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_DEV.TRANSACTIONS;
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_DEV.FEATURES;
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_DEV.ML;
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_DEV.SERVING;
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_DEV.MONITORING;

-- STAGING schemas
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_STAGING.TRANSACTIONS;
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_STAGING.FEATURES;
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_STAGING.ML;
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_STAGING.SERVING;
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_STAGING.MONITORING;

-- PROD schemas
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_PROD.TRANSACTIONS;
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_PROD.FEATURES;
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_PROD.ML;
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_PROD.SERVING;
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_PROD.MONITORING;

-- =============================================================================
-- SECTION 3: WAREHOUSES
-- =============================================================================
-- Each warehouse is right-sized for its specific workload pattern.
-- This avoids the anti-pattern of one oversized warehouse for everything.

-- FRAUD_DEMO_LOAD_WH: Standard LARGE (8 credits/hr)
-- PURPOSE: One-time bulk data generation (12M rows)
-- WHY LARGE: Processes 12M rows in ~2-3 minutes vs 10+ on MEDIUM.
--   At AUTO_SUSPEND=60s, total cost for a single load = ~0.3 credits ($1.37).
--   A smaller warehouse would take longer, costing similar credits but wasting time.
-- INITIALLY_SUSPENDED: Only starts when first used (no idle cost).
CREATE WAREHOUSE IF NOT EXISTS FRAUD_DEMO_LOAD_WH
    WAREHOUSE_SIZE = 'LARGE'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'One-time bulk data generation. LARGE for fast 12M row processing. Suspend after use.';

-- FRAUD_DEMO_WH: Standard SMALL (2 credits/hr)
-- PURPOSE: Dynamic Table refresh + general queries + Feature Store operations
-- WHY SMALL: Each DT refresh processes only ~46 new rows per entity per minute
--   (66k txns/day ÷ 24hrs ÷ 60min = 46 rows). SMALL handles this trivially.
--   Over-provisioning to MEDIUM would waste 2 extra credits/hr for no benefit.
-- FUTURE OPTIMISATION: If DT refresh time exceeds TARGET_LAG under load,
--   scale to MEDIUM. Monitor via SHOW DYNAMIC TABLES and check refresh_interval_seconds.
CREATE WAREHOUSE IF NOT EXISTS FRAUD_DEMO_WH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'DT refresh + general queries. SMALL is right-sized for micro-batch DT refreshes (~46 rows/min).';

-- FRAUD_DEMO_TRAIN_WH: Snowpark-Optimized MEDIUM (6 credits/hr, 256GB dedicated RAM)
-- PURPOSE: ML model training (XGBoost on 12M rows x 170 features)
-- WHY SNOWPARK-OPTIMIZED:
--   - Standard warehouses share memory with the query engine; OOM risk at this scale
--   - Snowpark-optimized MEDIUM provides 256GB RAM DEDICATED to the Python process
--   - 12M x 170 features ≈ 15GB in memory as float64 — fits comfortably with room for
--     XGBoost's internal histogram buffers (~2-3x data size)
-- WHY NOT STANDARD XLARGE (16 credits/hr):
--   - SP-Opt MEDIUM at 6 credits/hr is 62% cheaper AND provides more usable memory
--   - Standard XLARGE has 128GB total but shares with query engine = ~80GB usable
-- MAX_CONCURRENCY_LEVEL=1: Ensures the training job gets exclusive access to all resources.
--   Multiple concurrent training jobs would compete for RAM and slow each other down.
-- PERFORMANCE: Training completes in ~3-5 minutes (vs hours on undersized instances).
--   Cost per training run: ~0.5 credits ($2.29). Monthly retrain = $27/year.
-- INITIALLY_SUSPENDED: Only starts when training begins. Suspends 60s after completion.
CREATE WAREHOUSE IF NOT EXISTS FRAUD_DEMO_TRAIN_WH
    WAREHOUSE_SIZE = 'MEDIUM'
    WAREHOUSE_TYPE = 'SNOWPARK-OPTIMIZED'
    MAX_CONCURRENCY_LEVEL = 1
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'ML training. SP-Optimized MEDIUM = 256GB dedicated RAM at 6 credits/hr. Cheaper AND more memory than std XLARGE.';

-- =============================================================================
-- SECTION 4: COMPUTE POOL (SPCS Model Serving)
-- =============================================================================
-- FRAUD_DEMO_CPU_POOL: CPU_X64_XS (0.06 credits/hr per node)
-- PURPOSE: Host the XGBoost fraud scoring endpoint
-- WHY CPU_X64_XS (not CPU_X64_S at 0.11 credits/hr):
--   - XGBoost inference is CPU-bound but lightweight (~50ms per prediction)
--   - CPU_X64_XS provides 2 vCPUs + 8GB RAM — sufficient for a loaded XGBoost model
--     (~500MB in memory for 170 features) plus request handling overhead
--   - At 66k txns/day = 0.76 txns/sec average, 10x peak = 7.6 txns/sec
--     A single XS node handles 20 req/sec at 50ms each — well within capacity
-- WHY MAX_NODES=2:
--   - Provides burst capacity + high availability (if one node restarts, other serves)
--   - Second node only spins up under sustained load, otherwise idle (no cost)
-- ANNUAL SAVING vs CPU_X64_S: (0.11 - 0.06) × 8760 = 438 credits = ~$2,006/year
-- FUTURE OPTIMISATION: If P99 latency exceeds 100ms under peak load, scale to CPU_X64_S.
--   Monitor via service metrics (request_latency_p99).
CREATE COMPUTE POOL IF NOT EXISTS FRAUD_DEMO_CPU_POOL
    MIN_NODES = 1
    MAX_NODES = 2
    INSTANCE_FAMILY = CPU_X64_XS
    COMMENT = 'Fraud model serving. XS is right-sized for XGBoost inference. Saves $2k/yr vs S.';

-- =============================================================================
-- SECTION 5: ROLES & GRANTS
-- =============================================================================
-- Two-role pattern mirrors real ML team structure:
--   FRAUD_DS_DEV: Data scientists — full access to DEV, read-only on STAGING/PROD
--   FRAUD_MLOPS: MLOps/platform — full access to all environments for promotion

CREATE ROLE IF NOT EXISTS FRAUD_DS_DEV;
CREATE ROLE IF NOT EXISTS FRAUD_MLOPS;

-- Role hierarchy: both report to SYSADMIN
GRANT ROLE FRAUD_DS_DEV TO ROLE SYSADMIN;
GRANT ROLE FRAUD_MLOPS TO ROLE SYSADMIN;

-- Grant current user both roles for demo purposes
GRANT ROLE FRAUD_DS_DEV TO USER ASHAIKH;
GRANT ROLE FRAUD_MLOPS TO USER ASHAIKH;

-- FRAUD_DS_DEV: Full access to DEV, read on STAGING/PROD
GRANT ALL ON DATABASE FRAUD_DEMO_DEV TO ROLE FRAUD_DS_DEV;
GRANT ALL ON ALL SCHEMAS IN DATABASE FRAUD_DEMO_DEV TO ROLE FRAUD_DS_DEV;
GRANT USAGE ON DATABASE FRAUD_DEMO_STAGING TO ROLE FRAUD_DS_DEV;
GRANT USAGE ON ALL SCHEMAS IN DATABASE FRAUD_DEMO_STAGING TO ROLE FRAUD_DS_DEV;
GRANT USAGE ON DATABASE FRAUD_DEMO_PROD TO ROLE FRAUD_DS_DEV;
GRANT USAGE ON ALL SCHEMAS IN DATABASE FRAUD_DEMO_PROD TO ROLE FRAUD_DS_DEV;

-- FRAUD_MLOPS: Full access everywhere (promotes models, manages endpoints)
GRANT ALL ON DATABASE FRAUD_DEMO_DEV TO ROLE FRAUD_MLOPS;
GRANT ALL ON ALL SCHEMAS IN DATABASE FRAUD_DEMO_DEV TO ROLE FRAUD_MLOPS;
GRANT ALL ON DATABASE FRAUD_DEMO_STAGING TO ROLE FRAUD_MLOPS;
GRANT ALL ON ALL SCHEMAS IN DATABASE FRAUD_DEMO_STAGING TO ROLE FRAUD_MLOPS;
GRANT ALL ON DATABASE FRAUD_DEMO_PROD TO ROLE FRAUD_MLOPS;
GRANT ALL ON ALL SCHEMAS IN DATABASE FRAUD_DEMO_PROD TO ROLE FRAUD_MLOPS;

-- Warehouse grants
GRANT USAGE ON WAREHOUSE FRAUD_DEMO_LOAD_WH TO ROLE FRAUD_DS_DEV;
GRANT USAGE ON WAREHOUSE FRAUD_DEMO_WH TO ROLE FRAUD_DS_DEV;
GRANT USAGE ON WAREHOUSE FRAUD_DEMO_TRAIN_WH TO ROLE FRAUD_DS_DEV;
GRANT USAGE ON WAREHOUSE FRAUD_DEMO_LOAD_WH TO ROLE FRAUD_MLOPS;
GRANT USAGE ON WAREHOUSE FRAUD_DEMO_WH TO ROLE FRAUD_MLOPS;
GRANT USAGE ON WAREHOUSE FRAUD_DEMO_TRAIN_WH TO ROLE FRAUD_MLOPS;

-- Compute pool grants
GRANT USAGE ON COMPUTE POOL FRAUD_DEMO_CPU_POOL TO ROLE FRAUD_MLOPS;
GRANT MONITOR ON COMPUTE POOL FRAUD_DEMO_CPU_POOL TO ROLE FRAUD_DS_DEV;

-- Future grants (objects created later inherit these permissions)
GRANT ALL ON FUTURE TABLES IN DATABASE FRAUD_DEMO_DEV TO ROLE FRAUD_DS_DEV;
GRANT ALL ON FUTURE DYNAMIC TABLES IN DATABASE FRAUD_DEMO_DEV TO ROLE FRAUD_DS_DEV;
GRANT ALL ON FUTURE VIEWS IN DATABASE FRAUD_DEMO_DEV TO ROLE FRAUD_DS_DEV;
GRANT SELECT ON FUTURE TABLES IN DATABASE FRAUD_DEMO_STAGING TO ROLE FRAUD_DS_DEV;
GRANT SELECT ON FUTURE TABLES IN DATABASE FRAUD_DEMO_PROD TO ROLE FRAUD_DS_DEV;

-- =============================================================================
-- SECTION 6: STAGES (for model artifacts)
-- =============================================================================
CREATE STAGE IF NOT EXISTS FRAUD_DEMO_DEV.ML.MODEL_STAGE
    COMMENT = 'Model artifacts, training logs, experiment metadata';
CREATE STAGE IF NOT EXISTS FRAUD_DEMO_STAGING.ML.MODEL_STAGE
    COMMENT = 'Validated model artifacts awaiting production deployment';
CREATE STAGE IF NOT EXISTS FRAUD_DEMO_PROD.ML.MODEL_STAGE
    COMMENT = 'Production model artifacts';

-- =============================================================================
-- SETUP COMPLETE
-- =============================================================================
-- Next steps:
--   1. Run nb01_data_generation.ipynb (generates 12M transactions)
--   2. Run nb02_feature_engineering.ipynb (creates Dynamic Tables + Feature Store)
--   3. Run nb03_training.ipynb (trains XGBoost fraud model)
--   4. Run nb04_serving.ipynb (deploys SPCS endpoint)
--   5. Run nb05_monitoring.ipynb (sets up drift monitoring)
