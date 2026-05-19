# Fraud Detection ML Demo

Real-time fraud detection pipeline on Snowflake, replacing SageMaker + Redis + Spark Streaming with a unified platform. Demonstrates sub-minute feature freshness, production-grade model serving, and automated monitoring.

## Architecture

```
Transaction Ingestion:
  Back-end Service --> INSERT/Snowpipe Streaming --> FRAUD_TRANSACTIONS table
                                                        |
                                                        v (CLUSTER BY transaction_ts)
Feature Store (Dynamic Tables, TARGET_LAG = 1 minute):
  FRAUD_TRANSACTIONS --> CUSTOMER_VELOCITY (65 features, 5 windows)
                     --> MERCHANT_VELOCITY (20 features, 5 windows)
                     --> WALLET_DPAN_VELOCITY (15 features, 5 windows)
                     --> IP_VELOCITY (12 features, variable windows)
                     --> CUSTOMER_MERCHANT_VELOCITY (10 features, 5 windows)

Model Serving (SPCS, Pattern A -- features passed in request):
  Pre-computed features --> FRAUD_SCORING_SERVICE (CPU_X64_XS) --> fraud_probability

Monitoring:
  Inference log --> Model Monitor (AUC-PR + PSI) --> Alert --> Retrain Task
```

## Quick Start

```bash
# 1. Run infrastructure setup
snow sql -f scripts/setup.sql

# 2. Execute notebooks in order
# nb01: Data generation (12M transactions, ~3 min on LARGE WH)
# nb02: Feature engineering (Dynamic Tables + Feature Store)
# nb03: Model training (XGBoost, ~5 min on SP-Optimized MEDIUM)
# nb04: Model serving (SPCS deployment + latency benchmarks)
# nb05: Monitoring (drift detection + cost analysis)

# 3. Teardown (when done)
snow sql -f scripts/teardown.sql
```

## Warehouse Strategy (Cost-Optimised)

| Warehouse | Type | Size | Credits/hr | Purpose |
|-----------|------|------|-----------|---------|
| FRAUD_DEMO_LOAD_WH | Standard | LARGE | 8 | One-time data generation (12M rows in ~3 min) |
| FRAUD_DEMO_WH | Standard | SMALL | 2 | DT refresh + general queries (~46 rows/min per entity) |
| FRAUD_DEMO_TRAIN_WH | Snowpark-Optimized | MEDIUM | 6 | ML training (256GB dedicated RAM, MAX_CONCURRENCY=1) |

| Compute Pool | Instance | Credits/hr | Purpose |
|-------------|----------|-----------|---------|
| FRAUD_DEMO_CPU_POOL | CPU_X64_XS | 0.06 | Model serving (XGBoost inference, ~50ms/request) |

## Annual Cost Estimate

| Component | Annual Credits | Annual Cost (@$4.58) |
|-----------|---------------|---------------------|
| DT refresh (SMALL WH, 5 DTs) | 730 | $3,343 |
| SPCS endpoint (CPU_X64_XS, 24/7) | 526 | $2,409 |
| Training (monthly retrain, SP-Opt MEDIUM) | 6 | $27 |
| **Total** | **~1,262** | **~$5,780/yr** |

vs. current multi-system stack (SageMaker + Redis + Spark + DynamoDB): ~$20-40k/yr

## Key Metrics

| Metric | Result |
|--------|--------|
| Feature freshness | 30-60 seconds (vs 24 hours with daily dbt) |
| Model scoring latency | ~50ms median, <100ms P99 |
| Concurrent throughput | 20 req/sec per node |
| Training time | ~3-5 minutes (vs hours on SageMaker) |
| Fraud detection rate | ~80% recall at chosen operating point |
| Feature count | 170+ (5 entities x 5 time windows + derived) |

## Entity Model

| Entity | Cardinality | Key Velocity Features |
|--------|------------|----------------------|
| Customer | 200k | 13 metrics x 5 windows = 65 features |
| Merchant | 5k | 4 metrics x 5 windows = 20 features |
| Wallet DPAN | 50k | 3 metrics x 5 windows = 15 features |
| IP Address | 10k | 3 metrics x variable windows = 12 features |
| Customer-Merchant | compound | 2 metrics x 5 windows = 10 features |

## Project Structure

```
fraud_detection_ml/
├── README.md                           # This file
├── docs/
│   ├── architecture.md                 # Detailed architecture + DT DAG
│   ├── feature_catalogue.md            # Full 170+ feature specification
│   └── talk_track.md                   # Presenter timing guide
├── scripts/
│   ├── setup.sql                       # Infrastructure (DBs, WHs, roles, pool)
│   └── teardown.sql                    # Clean removal of all objects
└── notebooks/
    ├── nb01_data_generation.ipynb      # 12M synthetic transactions
    ├── nb02_feature_engineering.ipynb   # Dynamic Tables + Feature Store
    ├── nb03_training.ipynb             # XGBoost + Model Registry
    ├── nb04_serving.ipynb              # SPCS + latency benchmarks
    └── nb05_monitoring.ipynb           # Drift detection + cost analysis
```

## Design Decisions

1. **5 DTs (not 25)**: One DT per entity computes all 5 windows in a single GROUP BY pass. 80% cost reduction vs separate DTs per window.
2. **Snowpark-Optimized for training**: 256GB dedicated RAM at 6 credits/hr. Cheaper AND more memory than standard XLARGE (16 credits/hr).
3. **CPU_X64_XS for serving**: Right-sized for XGBoost inference. Saves $2k/yr vs CPU_X64_S.
4. **CLUSTER BY (transaction_ts)**: DT refreshes only read recent micro-partitions, not full table scan.
5. **Pattern A (stateless endpoint)**: Features pre-computed by DTs and passed in request. Endpoint does pure ML inference -- fastest possible.
6. **scale_pos_weight=2000**: Handles 0.05% fraud rate without memory-expensive oversampling.
7. **AUC-PR metric**: Appropriate for extreme class imbalance (ROC-AUC is misleading at 0.05%).
