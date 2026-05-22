# Real-Time Fraud Detection on Snowflake

> End-to-end ML fraud detection pipeline — from raw transactions to real-time scoring in under 200ms — built entirely on Snowflake. Replaces a multi-service architecture (SageMaker + Redis + Spark Streaming) with a single platform.

---

## Table of Contents

1. [Problem Statement](#problem-statement)
2. [Proposed Architecture](#proposed-architecture)
3. [Key Results](#key-results)
4. [Prerequisites](#prerequisites)
5. [Setup Guide](#setup-guide)
6. [Notebook Walkthrough](#notebook-walkthrough)
7. [Warehouse & Compute Strategy](#warehouse--compute-strategy)
8. [Design Decisions](#design-decisions)
9. [Supporting Documentation](#supporting-documentation)
10. [Teardown](#teardown)

---

## Problem Statement

### Current State (Multi-Service Architecture)

| Component | Technology | Pain Point |
|-----------|-----------|------------|
| Feature computation | Spark Streaming + dbt (daily) | 24-hour stale features miss rapid card-testing attacks |
| Feature serving | Redis / DynamoDB | Separate infra to maintain, sync issues, cache invalidation bugs |
| Model training | SageMaker | Slow iteration (~hours), data must leave Snowflake |
| Model serving | SageMaker endpoint | Low latency (~20ms) but features are 24h stale |
| Monitoring | Custom CloudWatch + manual | No automated drift detection or retraining |

### Core Challenges

- **Stale features**: Daily dbt refresh means velocity features (e.g., "purchases in last hour") are up to 24 hours old. Card-testing attacks complete in 30 seconds — the model never sees them.
- **Infrastructure sprawl**: 5+ services to coordinate (Snowflake → Spark → Redis → SageMaker → CloudWatch). Each introduces failure modes and sync lag.
- **Slow iteration**: Training requires data export, environment setup, and SageMaker orchestration. A single experiment takes hours.
- **No automated recovery**: When model performance degrades, the team discovers it manually days later.

### What Success Looks Like

| Requirement | Target |
|-------------|--------|
| Feature freshness | < 60 seconds (catch rapid card-testing) |
| Scoring latency | < 500ms P95 (real-time decisioning) |
| Training iteration | < 10 minutes (rapid experimentation) |
| Infrastructure | Single platform (reduce operational overhead) |
| Monitoring | Automated drift detection + retraining triggers |

---

## Proposed Architecture

```
Customer taps card
  │
  ▼
① Transaction lands in Snowflake              (~sub-second, Snowpipe Streaming)
  │
  ▼
② Dynamic Tables refresh velocity features    (~20-40 seconds)
  │
  ▼
③ Feature lookup from pre-computed DTs        (~15ms point query)
  │
  ▼
④ Score via SPCS model endpoint               (~105ms median via PrivateLink)
  │
  ▼
⑤ Decision: approve / flag / block            (instant)
```

### Platform Components

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        SNOWFLAKE (Single Platform)                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  INGESTION          FEATURES             ML                 SERVING       │
│  ┌──────────┐      ┌──────────────┐     ┌────────────┐    ┌──────────┐  │
│  │Snowpipe  │ ───► │Dynamic Tables│ ──► │Model       │ ──►│SPCS      │  │
│  │Streaming │      │(5 entities,  │     │Registry    │    │Container │  │
│  │          │      │ 1-min lag)   │     │(XGBoost)   │    │(REST API)│  │
│  └──────────┘      └──────────────┘     └────────────┘    └──────────┘  │
│                           │                    │                  │       │
│                           ▼                    ▼                  ▼       │
│                    ┌──────────────┐     ┌────────────┐    ┌──────────┐  │
│                    │Feature Store │     │Model       │    │Inference │  │
│                    │(147 features)│     │Monitor     │    │Log       │  │
│                    └──────────────┘     │(AUC-PR+PSI)│    └──────────┘  │
│                                         └────────────┘                   │
│                                                                           │
└─────────────────────────────────────────────────────────────────────────┘
```

### Production Deployment (Recommended)

```
Payment Gateway
      │
      ▼
AWS API Gateway (auth, WAF, rate-limiting, CloudTrail)
      │
      ▼  (AWS PrivateLink — no public internet)
      │
SPCS Container (private endpoint)
      │
      ├── Reads features from Dynamic Table (always current, <60s fresh)
      ├── Runs XGBoost inference (~100ms)
      │
      ▼
Approve / Flag / Block
```

---

## Key Results

| Metric | Before (SageMaker + Redis) | After (Snowflake) |
|--------|---------------------------|-------------------|
| Feature freshness | 24 hours | 20-40 seconds |
| Scoring latency | ~20ms (but stale features) | ~115ms (always-fresh features) |
| Training time | Hours | 3-5 minutes |
| Infrastructure components | 5+ services | 1 platform |
| Monthly infra cost | ~$14,500+ (AWS + Snowflake + Redis) | ~$13,388 (Snowflake only) |
| Fraud detection (card testing) | Missed (features too stale) | Caught within 1 DT refresh cycle |
| Model monitoring | Manual (days to detect degradation) | Automated (alert within hours of label arrival) |

### Scoring Path Options

| Path | Median Latency | P95 | Best For |
|------|---------------|-----|----------|
| Direct HTTP via PrivateLink | ~115ms | ~270ms | Real-time decisioning (sub-500ms SLA) |
| SQL service function | ~530ms | ~1,090ms | Async pipelines, batch re-scoring |

---

## Prerequisites

| Requirement | Details |
|-------------|---------|
| Snowflake Edition | Enterprise or higher |
| Role | ACCOUNTADMIN (for initial setup) |
| Snowflake CLI | `snow` CLI installed ([install guide](https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation/installation)) |
| Snowpark ML | `snowflake-ml-python` >= 1.5.4 |
| Region | Any AWS region (PrivateLink for production) |

---

## Setup Guide

### Step 1: Infrastructure Setup

Run the setup script to create all databases, warehouses, roles, schemas, and compute pools:

```bash
snow sql -f scripts/setup.sql
```

This creates:
- 3 databases: `FRAUD_DEMO_DEV`, `FRAUD_DEMO_STAGING`, `FRAUD_DEMO_PROD`
- 3 warehouses (right-sized per workload)
- 1 SPCS compute pool
- Schemas for each workload stage (TRANSACTIONS, FEATURES, ML, SERVING, MONITORING)

### Step 2: Execute Notebooks (in order)

Run each notebook sequentially in Snowsight or your local environment. Each notebook is self-contained with context setup at the top.

| # | Notebook | Duration | Warehouse Used |
|---|----------|----------|----------------|
| 1 | `nb01_data_generation.ipynb` | ~3 min | FRAUD_DEMO_LOAD_WH (LARGE) |
| 2 | `nb02_feature_engineering.ipynb` | ~5 min | FRAUD_DEMO_WH (MEDIUM) |
| 3 | `nb03_training.ipynb` | ~5 min | FRAUD_DEMO_TRAIN_WH (SP-Opt MEDIUM) |
| 4 | `nb04_serving.ipynb` | ~10 min | FRAUD_DEMO_CPU_POOL (SPCS) |
| 5 | `nb05_monitoring.ipynb` | ~5 min | FRAUD_DEMO_WH (SMALL) |

### Step 3: Teardown (when done)

```bash
snow sql -f scripts/teardown.sql
```

Removes all objects created by this demo. No orphaned resources.

---

## Notebook Walkthrough

### Notebook 1: Synthetic Data Generation

**Purpose:** Generate 12M training transactions (6 months) + 500k inference transactions (1 week) that replicate production entity volumes and fraud patterns.

**What it does:**
- Creates dimension tables for all 5 entities (Customer, Merchant, Wallet DPAN, IP, Card Token)
- Generates 12M rows in 4 batches of 3M (avoids memory pressure)
- Applies realistic fraud patterns at 0.05% rate (~6,000 fraud cases)
- Clusters table by `transaction_ts` for downstream DT efficiency

**Key design choice:** 0.05% fraud rate (1 in 2,000) matches production exactly. This extreme imbalance drives all downstream model decisions (scale_pos_weight, AUC-PR metric, threshold tuning).

---

### Notebook 2: Feature Engineering (Dynamic Tables + Feature Store)

**Purpose:** Build the real-time feature layer — 5 entity-level Dynamic Tables computing 147 features with sub-minute freshness.

**What it does:**
- Creates 5 entity DTs (one per entity, all 5 time windows in a single GROUP BY pass)
- Creates a combined features DT (joins entities + computes derived features)
- Registers Feature Store entities and Feature Views
- Runs a live freshness benchmark: INSERT → poll → measure actual DT lag

**Key design choice:** 5 DTs (not 25). One DT per entity computes all windows in one pass using conditional aggregation = 80% cost reduction vs separate DTs per window.

**Feature entities:**

| Entity | Features | Time Windows |
|--------|----------|--------------|
| Customer | 65 | 1h, 6h, 24h, 48h, 1wk |
| Merchant | 20 | 1h, 6h, 24h, 48h, 1wk |
| Wallet DPAN | 15 | 1h, 6h, 24h, 48h, 1wk |
| IP Address | 12 | Variable |
| Customer-Merchant | 10 | 1h, 6h, 24h, 48h, 1wk |

---

### Notebook 3: Model Training (XGBoost)

**Purpose:** Train an XGBoost fraud classifier on 12M transactions with 147 features, handling extreme class imbalance.

**What it does:**
- Loads training data via Feature Store joins
- Trains XGBoost with `scale_pos_weight=2000` (no oversampling needed)
- Evaluates with AUC-PR (not ROC-AUC — appropriate for extreme imbalance)
- Registers model in Snowflake Model Registry
- Promotes model: DEV → STAGING → PROD

**Key design choice:** Snowpark-Optimized MEDIUM warehouse (256GB dedicated RAM at 6 credits/hr). Cheaper than Standard XLARGE (16 credits/hr) with more usable memory.

**Result:** ~80% recall at chosen operating point. Training cost: ~0.5 credits (~$2.29) per run.

---

### Notebook 4: Model Serving & Latency Benchmarks (SPCS)

**Purpose:** Deploy the model as a REST endpoint on Snowpark Container Services and benchmark real-world scoring latency.

**What it does:**
- Deploys model to SPCS via Model Registry `create_service()`
- Benchmarks two scoring paths (SQL service function vs direct HTTP)
- Runs sustained load test (60 txn/min) and burst test (10 concurrent x 10 bursts)
- Measures cold-start, warm median, P95, P99 for each path
- Provides production architecture recommendation

**Key findings:**

| Metric | SQL Path | HTTP Path (PrivateLink est.) |
|--------|---------|-------------------------------|
| Warm Median | 529ms | ~115ms |
| P95 | 1,090ms | ~270ms |
| P95 < 500ms SLA | FAIL | PASS |

**Recommendation:** Direct HTTP via PrivateLink for real-time decisioning. SQL path for async/batch workloads.

---

### Notebook 5: Monitoring, Drift Detection & Cost Analysis

**Purpose:** Set up production monitoring — inference logging, performance tracking, drift detection, automated retraining, and cost/benefit analysis.

**What it does:**
- Creates inference logging table (predictions + features for audit)
- Simulates chargeback label arrival (24-72 hour delay)
- Creates Model Monitor (AUC-PR baseline + PSI drift detection)
- Analyses DT compute cost at different TARGET_LAG settings
- Compares before (daily dbt) vs after (1-minute DT) economics

**Key insight:** At 66k txns/day with $200 avg fraud loss, catching just 2 extra fraud cases/day pays for the entire DT compute pipeline ($9.30/day). The ROI is immediate.

---

## Warehouse & Compute Strategy

| Resource | Type | Size | Credits/hr | Purpose | When Active |
|----------|------|------|-----------|---------|-------------|
| FRAUD_DEMO_LOAD_WH | Standard | LARGE | 8 | Data generation (12M rows) | One-time (~3 min) |
| FRAUD_DEMO_WH | Standard | SMALL | 2 | DT refresh + general queries | 24/7 (DT pipeline) |
| FRAUD_DEMO_TRAIN_WH | Snowpark-Optimized | MEDIUM | 6 | ML training (256GB RAM) | Monthly (~5 min/run) |
| FRAUD_DEMO_CPU_POOL | SPCS | CPU_X64_XS | 0.06 | Model serving (REST endpoint) | 24/7 (always warm) |

**Monthly cost estimate:** ~$13,388 (dominated by DT warehouse running 24/7 at $13,190/month).

---

## Design Decisions

| # | Decision | Choice | Rationale |
|---|----------|--------|-----------|
| 1 | 5 DTs (not 25) | One DT per entity, all windows in single pass | 80% cost reduction vs separate DTs per window |
| 2 | Snowpark-Optimized for training | MEDIUM (6 credits/hr, 256GB RAM) | Cheaper AND more memory than Standard XLARGE (16 credits/hr) |
| 3 | CPU_X64_XS for serving | Smallest SPCS instance | Right-sized for XGBoost inference. Saves ~$2k/yr vs CPU_X64_S |
| 4 | CLUSTER BY (transaction_ts) | Linear clustering on timestamp | DT refreshes only read recent micro-partitions, not full table scan |
| 5 | Pattern A (stateless endpoint) | Features pre-computed, passed in request | Endpoint does pure ML inference — fastest possible path |
| 6 | scale_pos_weight=2000 | Inverse of fraud rate | Handles 0.05% fraud without memory-expensive oversampling |
| 7 | AUC-PR metric | Not ROC-AUC | Appropriate for extreme class imbalance (ROC-AUC is misleading) |
| 8 | PrivateLink (production) | No public ingress | Security + low latency. No data leaves Snowflake's network |

---

## Supporting Documentation

| Document | Description |
|----------|-------------|
| [`docs/architecture.md`](docs/architecture.md) | Detailed architecture diagrams, DT DAG, production deployment pattern, latency trade-offs, and deployment checklist |
| [`docs/feature_catalogue.md`](docs/feature_catalogue.md) | Full specification of all 170+ features (147 used at scoring time), grouped by entity with computation details |
| [`scripts/setup.sql`](scripts/setup.sql) | Infrastructure-as-code: all databases, warehouses, roles, schemas, compute pools |
| [`scripts/teardown.sql`](scripts/teardown.sql) | Clean removal of all demo objects |

---

## Teardown

To remove all resources created by this demo:

```bash
snow sql -f scripts/teardown.sql
```

This drops all 3 databases, all warehouses, and the compute pool. No orphaned objects remain.

---

## Project Structure

```
fraud_detection_ml/
├── README.md                              # This file (start here)
├── docs/
│   ├── architecture.md                    # Detailed architecture + production patterns
│   └── feature_catalogue.md              # Full 170+ feature specification
├── scripts/
│   ├── setup.sql                          # Infrastructure setup (run first)
│   └── teardown.sql                       # Clean removal (run last)
└── notebooks/
    ├── nb01_data_generation.ipynb         # 12M synthetic transactions
    ├── nb02_feature_engineering.ipynb     # Dynamic Tables + Feature Store
    ├── nb03_training.ipynb               # XGBoost + Model Registry
    ├── nb04_serving.ipynb                # SPCS deployment + latency benchmarks
    └── nb05_monitoring.ipynb             # Drift detection + cost analysis
```
