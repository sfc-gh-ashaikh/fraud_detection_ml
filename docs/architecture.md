# Architecture: Fraud Detection Pipeline

## End-to-End Pipeline

```
Customer taps card
  │
  ▼
① Transaction lands in Snowflake          (~sub-second, Snowpipe Streaming)
  │
  ▼
② Dynamic Tables refresh velocity features (~20-40s)
  │
  ▼
③ Feature lookup: read pre-computed        (~15ms, point query on DT)
   features for this customer/merchant/IP
  │
  ▼
④ Score: send features to model endpoint   (~50-200ms via SPCS)
  │
  ▼
⑤ Decision: approve or block               (instant)
```

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
                              │     147 features at scoring time │
                              └────────────────┬────────────────┘
                                               │
                    ┌──────────────────────────▼──────────────────────┐
                    │         XGBoost Model (Model Registry)           │
                    │  DEV --> STAGING --> PROD promotion               │
                    └──────────────────────────┬──────────────────────┘
                                               │
                              ┌────────────────▼────────────────┐
                              │    FRAUD_SCORING_SERVICE (SPCS)  │
                              │    CPU_X64_XS, ~105ms median     │
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

## Model Serving: Two Scoring Paths

The SPCS endpoint supports two invocation methods with different latency profiles:

### Path A: SQL Service Function

```sql
SELECT FRAUD_DEMO_PROD.ML.FRAUD_SCORING_SERVICE!PREDICT(
    AMT_USD, PURCHASES_AMT_L1H, PURCHASES_NUM_L1H, ...
) AS prediction
FROM <feature_table>;
```

| | |
|---|---|
| **When to use** | Scoring triggered by data arrival (Dynamic Tables, Tasks, Streams). Everything stays in Snowflake. |
| **Advantages** | No infra to manage. Built-in auth via RBAC. Composable with SQL. |
| **Disadvantages** | Higher latency (~530ms median) due to SQL compilation + warehouse scheduling. |
| **Best for** | Near-real-time pipelines (1-2s lag acceptable), batch re-scoring. |

### Path B: Direct HTTP (PrivateLink)

```
POST https://<private-endpoint>/predict
Content-Type: application/json

{"data": [[0, 142.50, 287.30, 3, 95.10, 5, ...]]}
```

| | |
|---|---|
| **When to use** | Synchronous scoring at transaction time — payment gateway needs instant approve/decline. |
| **Advantages** | Low latency (~115ms via PrivateLink). No SQL overhead. REST API pattern. |
| **Disadvantages** | Requires PrivateLink or caller in same SPCS account. Region co-location critical. |
| **Best for** | Real-time fraud decisioning, sub-500ms SLA requirements. |

### Measured Performance (60 txn/min sustained)

| Metric | SQL Service Function | Direct HTTP (PrivateLink est.) |
|--------|---------------------|-------------------------------|
| Cold-start | 894 ms | ~110-120 ms |
| Warm Median | 529 ms | ~115 ms |
| Warm P95 | 1,090 ms | ~270 ms |
| Warm P99 | 1,257 ms | ~510 ms |
| SLA (P95 < 500ms) | FAIL | PASS |

The model itself runs in ~100ms. The SQL path adds ~424ms of overhead (query parsing, warehouse scheduling, serialization).

## Recommended Production Architecture

```
Payment Gateway
      |
      v
AWS API Gateway (auth, WAF, rate-limiting, CloudTrail)
      |
      v  (AWS PrivateLink -- no public internet)
      |
SPCS Container (private endpoint)
      |
      |--- Reads features from Dynamic Table (always current, <60s fresh)
      |--- Runs XGBoost inference (~100ms)
      |
      v
Approve / Flag / Block
```

| Requirement | How it's met |
|---|---|
| Low latency (~115ms) | SPCS via PrivateLink, no SQL overhead |
| Feature freshness (<60s) | DTs refresh every ~20-40s, SPCS reads directly |
| Security (no public exposure) | Private endpoint + PrivateLink + API Gateway WAF |
| PCI/FCA compliance | CloudTrail audit, no data leaves Snowflake |
| Scalability | SPCS auto-scales 1-2 instances on load |
| Model lifecycle | Snowflake ML Registry (versioning, monitoring, rollback) |
| Cost | ~$198/month SPCS + ~$13,190/month DT pipeline |

### Latency vs Freshness Trade-offs

| Option | Total Latency | Feature Freshness | Infrastructure | Monthly Cost |
|---|---|---|---|---|
| **A: SPCS via PrivateLink (recommended)** | ~115ms | Always current | Snowflake only | ~$13,388 |
| B: SageMaker + Snowflake query | ~150-250ms | Always current | AWS + Snowflake | ~$13,500+ |
| C: SageMaker + external cache | ~25ms | 60-120s stale | AWS + Snowflake + Redis | ~$14,500+ |

**Recommendation:** Option A. 115ms scoring with guaranteed fresh features, zero external infrastructure, and no public endpoint exposure. The 115ms vs 25ms difference is irrelevant for fraud decisioning — both are imperceptible at checkout. What matters is catching fraud, and Option A catches it faster because features are never stale.

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
│  │ CPU_X64_XS (0.06 credits/hr)   │  ~105ms median per prediction   │
│  │ MIN=1, MAX=2 nodes             │  20 req/sec capacity            │
│  │ Cost: ~$198/month (1 node 24/7)│                                  │
│  └─────────────────────────────────┘                                  │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘
```

## Data Flow: Transaction to Decision

```
1. Transaction arrives in Snowflake (via Snowpipe Streaming)
   └── Lands in FRAUD_TRANSACTIONS table (~sub-second)

2. Dynamic Tables refresh (every ~20-40 seconds)
   └── 5 entity DTs recompute velocity features for affected entities
   └── Only new/changed micro-partitions are scanned (clustering)

3. Scoring request arrives (Pattern A: features pre-computed)
   └── Calling system reads features from DTs (~15ms point query)
   └── Passes all 147 features in REST request body
   └── FRAUD_SCORING_SERVICE returns probability in ~105ms (HTTP path)

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

## Production Deployment Checklist

- [ ] Configure AWS PrivateLink between customer VPC and Snowflake
- [ ] Set up API Gateway with WAF rules and API key authentication
- [ ] Keep SPCS endpoint **private** (no public ingress in production)
- [ ] Set compute pool `min_instances=1` to avoid cold-starts
- [ ] Increase `CONCURRENT_REQUESTS_MAX` based on load testing
- [ ] Configure Model Monitor alerts (NB05) for drift detection
- [ ] Set up CI/CD pipeline for model version deployments via ML Registry
- [ ] Ensure caller is in same AWS region as Snowflake account (cross-region adds 2-3s)
