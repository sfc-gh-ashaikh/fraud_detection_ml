# Talk Track: Fraud Detection Demo

Presenter timing guide for the ~20 minute fraud detection demo. Covers which cells to pre-run vs run live, key talking points, and objection handling.

## Pre-Demo Setup (5 minutes before)

1. Run `scripts/setup.sql` (creates all infrastructure)
2. Run NB01 fully (data generation -- takes ~3 min)
3. Run NB02 cells 1-10 (DT creation -- let them bootstrap ~60s)
4. Pre-deploy SPCS endpoint (NB04 cell 2 -- takes ~2 min to pull container)
5. Open all 5 notebooks in tabs

## Demo Flow (~20 minutes)

### NB01: Data (2 min) -- PRE-RUN, walk through

| Time | Cell | Action | Talking Point |
|------|------|--------|---------------|
| 0:00 | All | Show pre-run results | "12M transactions across 5 entities, matching your exact production volumes" |
| 0:30 | Verify cell | Highlight fraud rate | "0.05% -- exactly your production rate. 6,000 fraud cases for training" |
| 1:00 | Cluster cell | Explain | "Clustering is critical -- without it, DTs scan 12M rows every minute. With it, they scan ~46 rows" |
| 1:30 | | Transition | "Data is ready. Now let's see how fast features can refresh" |

### NB02: Feature Engineering (5 min) -- MIX of pre-run + LIVE

| Time | Cell | Action | Talking Point |
|------|------|--------|---------------|
| 2:00 | DT creation | Show pre-run | "5 Dynamic Tables, one per entity. Each computes all 5 time windows in a SINGLE pass" |
| 2:30 | DT verify | Show counts | "All populated. Initial bootstrap took ~60s. Ongoing refreshes process only new rows" |
| 3:00 | | Explain architecture | "This replaces your daily dbt refresh. Same SQL logic, 1440x fresher. Zero orchestration" |
| 3:30 | Freshness benchmark | **RUN LIVE** | "Watch this -- I'm inserting 10 transactions NOW. Let's see how fast the DT picks them up" |
| 5:00 | | Show result | "30-60 seconds. Your features are now at most 1 minute stale, not 24 hours" |
| 5:30 | Feature Store | Show pre-run | "All registered in Snowflake's Feature Store. Point-in-time correct training sets, versioned" |

### NB03: Training (3 min) -- PRE-RUN, walk through

| Time | Cell | Action | Talking Point |
|------|------|--------|---------------|
| 6:00 | WH choice | Highlight comment | "Snowpark-Optimized MEDIUM: 256GB dedicated RAM at 6 credits/hr. CHEAPER than standard XLARGE" |
| 6:30 | Training | Show timing | "Trained in 3-5 minutes. Your current SageMaker pipeline takes hours" |
| 7:00 | Evaluation | Show AUC-PR | "AUC-PR is the right metric at 0.05%. ROC-AUC would mislead you here" |
| 7:30 | Feature importance | Show top 20 | "These are the features catching fraud. Velocity in short windows dominates" |
| 8:00 | Promotion | **RUN LIVE** | "One API call: DEV to STAGING to PROD. No manual artifact management" |

### NB04: Serving (5 min) -- LIVE benchmarks

| Time | Cell | Action | Talking Point |
|------|------|--------|---------------|
| 9:00 | Deploy | Show pre-deployed | "Already running on CPU_X64_XS -- costs $0.27/hr. That's the smallest instance" |
| 9:30 | Single latency | **RUN LIVE** | "50 requests, let's see..." |
| 10:30 | | Show results | "~50ms median. Well within your latency budget" |
| 11:00 | Concurrent | **RUN LIVE** | "10 simultaneous requests -- simulating your 10x peak" |
| 12:00 | | Show results | "All under 100ms. No degradation under load" |
| 12:30 | | Transition | "Scoring is not the bottleneck. Feature freshness (60s) is the dominant latency" |

### NB05: Monitoring (5 min) -- MIX

| Time | Cell | Action | Talking Point |
|------|------|--------|---------------|
| 13:00 | Inference log | Show schema | "Every prediction logged with features. Audit trail, drift monitoring, performance tracking" |
| 13:30 | Monitor | **RUN LIVE** | "Model Monitor tracks AUC-PR against baseline. Alerts when performance degrades" |
| 14:00 | Cost analysis | Show table | "The key trade-off: freshness vs cost. 1-minute is the sweet spot for you" |
| 15:00 | Before/after | Show comparison | "Same SQL, 1440x fresher, 3 systems eliminated, 70-85% cost reduction" |
| 16:00 | Retrain Task | Show code | "Automatic monthly retraining. Accelerated if drift is detected. Self-healing pipeline" |

## Closing (2 min)

| Time | Talking Point |
|------|---------------|
| 17:00 | "Total annual cost: ~$5,780. Compare that to your current multi-system stack at $20-40k" |
| 17:30 | "Zero Airflow DAGs. Zero Redis. Zero Spark Streaming. One platform" |
| 18:00 | "Training in minutes, not hours. Features in seconds, not days. Scoring in milliseconds" |
| 18:30 | "Questions?" |

## Objection Handling

| Objection | Response |
|-----------|----------|
| "Is 1-minute staleness really OK?" | "You confirmed it is. But if needed, TARGET_LAG goes to 30s or 10s -- just costs more compute" |
| "What about cold start?" | "MIN_NODES=1 keeps endpoint warm. Zero cold start. Cost: $0.27/hr" |
| "Can it handle our peak?" | "CPU_X64_XS does 20 req/sec. Your peak is ~10/sec. 2x headroom on smallest instance" |
| "What about model explainability?" | "XGBoost provides feature importance. SHAP can be added in a stored procedure" |
| "How do we migrate from dbt?" | "Same SQL. Wrap your dbt model SQL in CREATE DYNAMIC TABLE. dbt can run as backup during transition" |
| "What about GPU training?" | "Supported via GPU_NV_S compute pool. Switch when you move to deep learning" |
