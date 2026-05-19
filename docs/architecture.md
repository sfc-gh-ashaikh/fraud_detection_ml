# Architecture: Fraud Detection Pipeline

## Entity Model

```
                    ┌─────────────────────────────────────────────────┐
                    │              FRAUD_TRANSACTIONS                   │
                    │  (12M rows, CLUSTER BY transaction_ts)           │
                    └─────────┬───────┬───────┬───────┬───────┬───────┘
                              │       │       │       │       │
                    ┌─────────▼──┐ ┌──▼─────┐ ┌▼──────┐ ┌───▼───┐ ┌──▼──────────┐
                    │ CUSTOMER   │ │MERCHANT│ │WALLET │ │  IP   │ │CUST-MERCHANT│
                    │ VELOCITY   │ │VELOCITY│ │ DPAN  │ │VELOC. │ │  VELOCITY   │
                    │ (65 cols)  │ │(20cols)│ │(15col)│ │(12col)│ │  (10 cols)  │
                    └─────────┬──┘ └──┬─────┘ └┬──────┘ └───┬───┘ └──┬──────────┘
                              │       │        │             │        │
                              └───────┴────────┴─────────────┴────────┘
                                              │
                              ┌────────────────▼────────────────┐
                              │     FEATURE STORE (Feature View) │
                              │     170+ features combined       │
                              └────────────────┬────────────────┘
                                               │
                    ┌──────────────────────────▼──────────────────────┐
                    │         XGBoost Model (Model Registry)           │
                    │  DEV --> STAGING --> PROD promotion               │
                    └──────────────────────────┬──────────────────────┘
                                               │
                              ┌────────────────▼────────────────┐
                              │    FRAUD_SCORING_SERVICE (SPCS)  │
                              │    CPU_X64_XS, ~50ms latency     │
                              └────────────────┬────────────────┘
                                               │
                              ┌────────────────▼────────────────┐
                              │     Model Monitor + Alerting      │
                              │     AUC-PR tracking, PSI drift    │
                              └─────────────────────────────────┘
```

## Dynamic Table DAG

Snowflake manages refresh ordering automatically:

```
FRAUD_TRANSACTIONS (source)
    │
    ├── CUSTOMER_VELOCITY         (TARGET_LAG = 1 min, SMALL WH)
    ├── MERCHANT_VELOCITY         (TARGET_LAG = 1 min, SMALL WH)
    ├── WALLET_DPAN_VELOCITY      (TARGET_LAG = 1 min, SMALL WH)
    ├── IP_VELOCITY               (TARGET_LAG = 1 min, SMALL WH)
    └── CUSTOMER_MERCHANT_VELOCITY (TARGET_LAG = 1 min, SMALL WH)
```

Each DT computes all 5 time windows (1h, 6h, 24h, 48h, 1wk) in a single GROUP BY pass using conditional aggregation. The outer WHERE clause limits the scan to 7 days max.

## Warehouse Strategy

```
┌─────────────────────────────────────────────────────────────────────┐
│                         WORKLOAD ROUTING                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  Data Generation (one-time)                                           │
│  ┌─────────────────────────────────┐                                  │
│  │ FRAUD_DEMO_LOAD_WH              │                                  │
│  │ Standard LARGE (8 credits/hr)   │  12M rows in ~3 min             │
│  │ AUTO_SUSPEND = 60s              │  Total cost: ~0.4 credits        │
│  │ INITIALLY_SUSPENDED             │                                  │
│  └─────────────────────────────────┘                                  │
│                                                                       │
│  DT Refresh + General (ongoing)                                       │
│  ┌─────────────────────────────────┐                                  │
│  │ FRAUD_DEMO_WH                   │                                  │
│  │ Standard SMALL (2 credits/hr)   │  ~46 rows/min per entity        │
│  │ AUTO_SUSPEND = 60s              │  Micro-batch optimised           │
│  └─────────────────────────────────┘                                  │
│                                                                       │
│  ML Training (monthly)                                                │
│  ┌─────────────────────────────────┐                                  │
│  │ FRAUD_DEMO_TRAIN_WH             │                                  │
│  │ SP-Optimized MEDIUM (6 cr/hr)   │  256GB dedicated RAM            │
│  │ MAX_CONCURRENCY_LEVEL = 1       │  ~5 min training time           │
│  │ AUTO_SUSPEND = 60s              │  Cost: ~0.5 credits/run         │
│  │ INITIALLY_SUSPENDED             │                                  │
│  └─────────────────────────────────┘                                  │
│                                                                       │
│  Model Serving (24/7)                                                 │
│  ┌─────────────────────────────────┐                                  │
│  │ FRAUD_DEMO_CPU_POOL             │                                  │
│  │ CPU_X64_XS (0.06 credits/hr)   │  ~50ms per prediction           │
│  │ MIN=1, MAX=2 nodes             │  20 req/sec capacity            │
│  └─────────────────────────────────┘                                  │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘
```

## Data Flow: Transaction to Decision

```
1. Transaction arrives in Snowflake (via existing ingestion)
   └── Lands in FRAUD_TRANSACTIONS table

2. Dynamic Tables refresh (every ~60 seconds)
   └── 5 entity DTs recompute velocity features for affected entities
   └── Only new/changed micro-partitions are scanned (clustering)

3. Scoring request arrives (Pattern A: features pre-computed)
   └── Calling system reads features from DTs
   └── Passes all 170+ features in REST request body
   └── FRAUD_SCORING_SERVICE returns probability in ~50ms

4. Decision made
   └── probability > threshold → block/review transaction
   └── Prediction logged to INFERENCE_LOG

5. Label arrives (24-72 hours later, via chargeback)
   └── INFERENCE_LOG updated with ground truth
   └── Model Monitor evaluates performance
   └── Alert fires if AUC-PR degrades > 5%
   └── Retrain Task triggers if needed
```

## Environment Promotion

```
FRAUD_DEMO_DEV                    FRAUD_DEMO_STAGING              FRAUD_DEMO_PROD
├── TRANSACTIONS (source data)    ├── ML (validated models)        ├── ML (production models)
├── FEATURES (DTs, Feature Store) ├── FEATURES (clone for test)    ├── SERVING (SPCS endpoint)
├── ML (experiments, models)      └── MONITORING (test monitors)   └── MONITORING (live monitors)
└── SERVING (dev endpoint)
```

Model promotion: `log_model()` in DEV → re-register in STAGING (validation) → re-register in PROD (serving).
