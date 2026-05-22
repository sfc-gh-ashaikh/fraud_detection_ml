# Real-Time Fraud Detection on Snowflake

> Catch fraud faster by keeping features fresh. This demo shows how sub-minute feature freshness — powered by Snowflake Dynamic Tables — directly improves fraud hit rates by detecting card-testing attacks that stale daily features miss entirely.

---

## Table of Contents

1. [Problem Statement](#problem-statement)
2. [Proposed Architecture](#proposed-architecture)
3. [Key Outcomes](#key-outcomes)
4. [Prerequisites](#prerequisites)
5. [Setup Guide](#setup-guide)
6. [Notebook Walkthrough](#notebook-walkthrough)
7. [Design Decisions](#design-decisions)
8. [Supporting Documentation](#supporting-documentation)
9. [Teardown](#teardown)

---

## Problem Statement

### The Business Problem

Fraud losses are directly tied to how fresh your features are.

A typical BNPL product processes ~66,000 transactions per day at a 0.05% fraud rate — roughly 33 fraud cases daily, each costing ~$200 on average. That's ~$6,600/day in fraud exposure. The most damaging pattern is **card testing**: fraudsters make 5-10 rapid purchases in under 30 seconds to validate stolen credentials before executing high-value transactions.

The model's ability to catch these attacks depends entirely on one thing: **can it see the velocity spike in time?**

### Why Feature Freshness Determines Fraud Hit Rate

The most predictive signals in fraud detection are velocity features — "how many purchases has this customer made in the last hour?" or "how many distinct merchants in the last 6 hours?" These features only work if they reflect what's happening *right now*.

| Feature Freshness | What the Model Sees | Fraud Outcome |
|-------------------|--------------------|--------------| 
| **24 hours (daily batch)** | Yesterday's activity | Card-testing attack completes undetected. Model sees zero velocity spike. All fraudulent transactions approved. |
| **< 60 seconds (Dynamic Tables)** | Current activity | After the 2nd-3rd rapid purchase, velocity features spike. Model flags remaining transactions in the burst. Attack interrupted. |
| **< 15 seconds (Streams + Tasks)** | Near-immediate activity | Model detects velocity spike after the 1st-2nd purchase. Earliest possible intervention. |

In concrete terms: a card-testing attack that completes in 30 seconds is **invisible** to a model running on 24-hour-old features. The velocity features (purchases_num_l1h, distinct_merchants_l1h) still show yesterday's values. The model has no signal to act on.

With sub-minute freshness, the same attack triggers velocity spikes within one refresh cycle. The model sees the burst developing and can block subsequent transactions before the fraudster completes their sequence.

### Current State vs Desired State

| | Current (Daily Batch) | Desired (Real-Time) |
|---|---|---|
| **Feature freshness** | 24 hours | < 60 seconds (or < 15 seconds for highest urgency) |
| **Card-testing detection** | Missed entirely | Caught mid-attack |
| **Fraud losses from velocity attacks** | Full exposure (~$6,600/day) | Reduced by catching attacks in-flight |
| **Model iteration speed** | Hours per experiment | Minutes |
| **Operational complexity** | 5+ services to coordinate | Single platform |
| **Drift detection** | Manual (days to notice) | Automated (hours after labels arrive) |

### The Core Insight

This isn't a model accuracy problem — it's a **data freshness problem**. The same model, with the same features and the same weights, produces dramatically different fraud hit rates depending solely on whether those features reflect what happened 24 hours ago or 30 seconds ago.

---

## Proposed Architecture

Two architectural patterns are provided, optimised for different freshness and latency requirements:

### Option A: Dynamic Tables (Recommended — NB02)

Best for most fraud use cases. Declarative, zero-maintenance, proven.

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
③ Feature lookup from pre-computed DTs        (warehouse query)
  │
  ▼
④ Score via SPCS model endpoint               (sub-second)
  │
  ▼
⑤ Decision: approve / flag / block            (instant)
```

### Option B: Streams + Tasks + Hybrid Tables (NB02b)

For when sub-15 second freshness and the lowest possible lookup latency are hard requirements. Trades operational simplicity for speed.

```
Customer taps card
  │
  ▼
① Transaction lands in Snowflake              (~sub-second, Snowpipe Streaming)
  │
  ▼
② Stream captures new row instantly           (zero lag)
  │
  ▼
③ Task fires every 10s, MERGEs into           (~10-15 seconds total)
   Hybrid Feature Tables (all 5 entities)
  │
  ▼
④ Scoring service reads Hybrid Tables         (~5-10ms per entity, indexed lookup)
   + runs model inference
  │
  ▼
⑤ Decision: approve / flag / block            (instant)
```

### Comparison

| | Dynamic Tables (Option A) | Streams + Tasks + Hybrid Tables (Option B) |
|---|---|---|
| Feature freshness | 33-39 seconds | 10-15 seconds |
| Feature lookup latency | Warehouse query | 5-10ms (Hybrid Table index) |
| Implementation | Declarative (write a SELECT) | Imperative (MERGE logic + dual-task pattern) |
| Maintenance | Zero (Snowflake-managed) | Medium (you own the procedures + monitoring) |
| Rolling window handling | Automatic (full recompute) | Manual (fast task adds, slow task expires) |
| Error recovery | Automatic | Manual (task error handling required) |
| Recommended when | 30-60s freshness is acceptable | Sub-15s freshness is a hard requirement |

### Platform Components

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        SNOWFLAKE (Single Platform)                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  INGESTION          FEATURES             ML                 SERVING       │
│  ┌──────────┐      ┌──────────────┐     ┌────────────┐    ┌──────────┐  │
│  │Snowpipe  │ ───► │Dynamic Tables│ ──► │Model       │ ──►│SPCS      │  │
│  │Streaming │      │  OR          │     │Registry    │    │Container │  │
│  │          │      │Stream + Task │     │(XGBoost)   │    │(REST API)│  │
│  │          │      │+ Hybrid Table│     │            │    │          │  │
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
AWS API Gateway (auth, WAF, rate-limiting)
      │
      ▼  (AWS PrivateLink — no public internet)
      │
SPCS Container (private endpoint)
      │
      ├── Reads features from Dynamic Table or Hybrid Table
      ├── Runs XGBoost inference
      │
      ▼
Approve / Flag / Block
```

---

## Key Outcomes

| Outcome | Before (Daily Batch) | After (Snowflake) |
|---------|---------------------|-------------------|
| Feature freshness | 24 hours | 20-40 seconds (DT) or 10-15 seconds (Streams + Tasks) |
| Card-testing detection | Missed (invisible to model) | Caught within one refresh cycle |
| Fraud hit rate (velocity attacks) | Near zero | Significantly improved |
| Time to retrain | Hours (data export + external tooling) | 3-5 minutes (in-platform) |
| Infrastructure complexity | 5+ services | 1 platform |
| Drift detection | Manual discovery (days) | Automated alerting (hours after label arrival) |
| Model monitoring | Ad-hoc | Continuous (AUC-PR baseline + PSI drift) |

---

## Prerequisites

| Requirement | Details |
|-------------|---------|
| Snowflake Edition | Enterprise or higher |
| Role | ACCOUNTADMIN (for initial setup) |
| Snowflake CLI | `snow` CLI installed ([install guide](https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation/installation)) |
| Snowpark ML | `snowflake-ml-python` >= 1.5.4 |
| Region | Any AWS region |

---

## Setup Guide

### Step 1: Infrastructure Setup

```bash
snow sql -f scripts/setup.sql
```

Creates all databases, warehouses, roles, schemas, and compute pools needed for the demo.

### Step 2: Execute Notebooks (in order)

Run each notebook sequentially in Snowsight or your local environment. Each is self-contained with context setup at the top.

| # | Notebook | Duration | What It Proves |
|---|----------|----------|----------------|
| 1 | `nb01_data_generation.ipynb` | ~3 min | Generates realistic 12M-transaction dataset with production fraud patterns |
| 2 | `nb02_feature_engineering.ipynb` | ~5 min | Sub-minute feature freshness via Dynamic Tables (measured, not theoretical) |
| 2b | `nb02b_streams_tasks_features.ipynb` | ~5 min | Sub-15s feature freshness via Streams + Tasks + Hybrid Tables (alternative to NB02) |
| 3 | `nb03_training.ipynb` | ~5 min | Full model training cycle in minutes, not hours |
| 4 | `nb04_serving.ipynb` | ~10 min | Real-time scoring with latency benchmarks |
| 5 | `nb05_monitoring.ipynb` | ~5 min | Automated drift detection + business case for freshness |

> **Note:** Run either NB02 or NB02b (or both to compare). NB02 is the recommended starting point. NB02b is an alternative for teams with a hard sub-15s freshness requirement.

### Step 3: Teardown (when done)

```bash
snow sql -f scripts/teardown.sql
```

---

## Notebook Walkthrough

### Notebook 1: Synthetic Data Generation

**Purpose:** Generate 12M training transactions (6 months) + 500k inference transactions (1 week) replicating production entity volumes and fraud patterns.

**What it does:**
- Creates dimension tables for all 5 entities (Customer, Merchant, Wallet DPAN, IP, Card Token)
- Generates 12M rows with realistic fraud patterns at 0.05% rate (~6,000 fraud cases)
- Clusters table by `transaction_ts` for downstream efficiency

**Why it matters:** The 0.05% fraud rate (1 in 2,000) creates the extreme class imbalance that makes fraud detection genuinely hard. This isn't a toy dataset — it replicates the real challenge.

---

### Notebook 2: Feature Engineering (Dynamic Tables + Feature Store)

**Purpose:** Build the real-time feature layer that makes card-testing detection possible. This is the core value driver of the architecture.

**What it does:**
- Creates 5 entity-level Dynamic Tables computing rolling velocity features across 5 time windows (1h, 6h, 24h, 48h, 1wk)
- Registers features in the Snowflake Feature Store
- Runs a live freshness benchmark: INSERT a transaction → measure how quickly features update

**Why it matters:** This is where the fraud hit rate improvement comes from. A feature like `purchases_num_l1h` (purchases in last hour) is the primary signal for card-testing detection. If it's 24 hours stale, it's useless. If it's 30 seconds stale, it catches the attack.

**Feature entities:**

| Entity | Features | What It Detects |
|--------|----------|-----------------|
| Customer | 65 | Unusual spending velocity, geographic anomalies |
| Merchant | 20 | Merchant under attack (many cards tested) |
| Wallet DPAN | 15 | Compromised card (shared across customers) |
| IP Address | 12 | Bot farm (many customers from single IP) |
| Customer-Merchant | 10 | Repeated rapid purchases at same merchant |

---

### Notebook 2b: Streams + Tasks + Hybrid Tables (Alternative)

**Purpose:** Achieve the fastest possible Snowflake-native feature freshness (10-15 seconds) for scenarios where sub-minute latency from Dynamic Tables isn't sufficient.

**What it does:**
- Creates a Stream on the transactions table (captures new rows instantly)
- Creates Hybrid Tables for all 5 entities (row-based, indexed for fast point lookups)
- Implements a dual-task pattern:
  - **Fast task (10s):** Incrementally MERGEs new transactions into feature tables
  - **Slow task (5 min):** Full recompute to handle rolling window expiry
- Benchmarks end-to-end freshness: INSERT → features available in 10-15 seconds
- Provides a direct comparison with NB02 (Dynamic Tables)

**Why it matters:** For the purchase-flow use case where a fraud decision must happen within the transaction approval window, every second counts. This pattern reduces feature staleness from ~35s (Dynamic Tables) to ~12s (Streams + Tasks), at the cost of increased operational complexity. The Hybrid Tables also enable 5-10ms indexed lookups at scoring time — eliminating the warehouse query overhead entirely.

**Trade-off:** Dynamic Tables are declarative and self-healing. Streams + Tasks require you to own the MERGE logic, handle task failures, and manage window expiry manually. Choose this path only when the freshness improvement justifies the operational investment.

---

### Notebook 3: Model Training (XGBoost)

**Purpose:** Train a fraud classifier and register it for production deployment — demonstrating that the full training cycle takes minutes, enabling rapid iteration.

**What it does:**
- Trains XGBoost on 12M transactions with 147 features
- Handles extreme class imbalance (0.05%) without oversampling
- Evaluates with fraud-appropriate metrics (AUC-PR)
- Registers model in Snowflake Model Registry with DEV → STAGING → PROD promotion

**Why it matters:** When fraud patterns shift (new attack vectors, seasonal changes), the team needs to retrain quickly. A 5-minute training cycle means same-day response to emerging threats, not week-long projects.

---

### Notebook 4: Model Serving & Latency Benchmarks (SPCS)

**Purpose:** Deploy the model as a REST endpoint and prove it can score transactions fast enough for real-time decisioning.

**What it does:**
- Deploys model to Snowpark Container Services via the Model Registry
- Benchmarks two scoring paths (SQL service function vs direct HTTP)
- Runs sustained and burst load tests at production volumes
- Provides production architecture recommendation with PrivateLink

**Why it matters:** A fraud decision must happen before the transaction is approved. This notebook proves the model can return a score fast enough to block in real-time — not just in theory, but under realistic concurrent load.

**Important context on latency:** Introducing a dynamic feature store (whether via Dynamic Tables or Hybrid Tables) means feature lookup adds latency that doesn't exist in a pure endpoint-only solution where features are pre-loaded into memory. This is an inherent trade-off regardless of architecture — you're trading a few milliseconds of lookup time for features that are seconds fresh instead of hours stale. The benchmarks in this notebook quantify that trade-off precisely.

---

### Notebook 5: Monitoring, Drift Detection & Cost Analysis

**Purpose:** Close the loop — ensure the model stays effective over time and quantify the business value of feature freshness.

**What it does:**
- Sets up inference logging for audit and performance tracking
- Creates a Model Monitor with automated drift detection (PSI)
- Alerts when model performance degrades (AUC-PR drop > 5%)
- Analyses the relationship between feature freshness and fraud losses avoided
- Provides cost estimation with ROI rationale for implementing real-time features

**Why it matters:** Fraud patterns evolve. Without automated monitoring, model degradation goes unnoticed for days — during which fraud losses mount. This notebook proves the system self-heals: detect drift → alert → trigger retrain → deploy updated model. The cost analysis demonstrates that the ROI of fresher features is immediate — catching just 2 additional fraud cases per day pays for the entire compute pipeline.

---

## Design Decisions

| # | Decision | Choice | Rationale |
|---|----------|--------|-----------|
| 1 | 5 DTs (not 25) | One DT per entity, all windows in single pass | Efficient compute — one table scan per entity per refresh |
| 2 | Snowpark-Optimized for training | MEDIUM with 256GB dedicated RAM | Faster training with more usable memory |
| 3 | CLUSTER BY (transaction_ts) | Linear clustering on timestamp | DT refreshes only read recent data, not full history |
| 4 | Pattern A (stateless endpoint) | Features pre-computed, passed in request | Fastest scoring path — endpoint does pure inference |
| 5 | scale_pos_weight=2000 | Inverse of fraud rate | Handles extreme imbalance without oversampling |
| 6 | AUC-PR metric | Not ROC-AUC | Meaningful at 0.05% fraud (ROC-AUC is misleading) |
| 7 | PrivateLink (production) | No public ingress | Data never leaves Snowflake's network |
| 8 | Automated retraining | Monthly + drift-triggered | Adapts to evolving fraud patterns without manual intervention |
| 9 | Dual-task pattern (NB02b) | Fast task (10s) + Slow task (5min) | Fast adds new activity instantly; slow handles window expiry correctly |
| 10 | Hybrid Tables for serving (NB02b) | Row-based with indexed primary keys | 5-10ms point lookups vs warehouse query overhead |

---

## Supporting Documentation

| Document | Description |
|----------|-------------|
| [`docs/architecture.md`](docs/architecture.md) | Detailed architecture diagrams, production deployment pattern, latency benchmarks, and deployment checklist |
| [`docs/feature_catalogue.md`](docs/feature_catalogue.md) | Full specification of all features, grouped by entity with computation details |
| [`scripts/setup.sql`](scripts/setup.sql) | Infrastructure setup: databases, warehouses, roles, schemas, compute pools |
| [`scripts/teardown.sql`](scripts/teardown.sql) | Clean removal of all demo objects |

---

## Teardown

```bash
snow sql -f scripts/teardown.sql
```

Removes all databases, warehouses, and compute pools. No orphaned objects.

---

## Project Structure

```
fraud_detection_ml/
├── README.md                              # This file (start here)
├── docs/
│   ├── architecture.md                    # Detailed architecture + production patterns
│   └── feature_catalogue.md              # Full feature specification
├── scripts/
│   ├── setup.sql                          # Infrastructure setup (run first)
│   └── teardown.sql                       # Clean removal (run last)
└── notebooks/
    ├── nb01_data_generation.ipynb         # 12M synthetic transactions
    ├── nb02_feature_engineering.ipynb     # Dynamic Tables + Feature Store (recommended)
    ├── nb02b_streams_tasks_features.ipynb # Streams + Tasks + Hybrid Tables (sub-15s alternative)
    ├── nb03_training.ipynb               # XGBoost + Model Registry
    ├── nb04_serving.ipynb                # SPCS deployment + latency benchmarks
    └── nb05_monitoring.ipynb             # Drift detection + cost analysis
```
