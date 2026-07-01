# Real-Time Fraud Detection — Lambda Architecture
## Complete 7-Day Implementation Plan

**Stack**: Kafka · Flink · Spark · Hive · Delta Lake · (Iceberg — stretch goal)
**Goal**: A defensible, end-to-end fraud detection system combining real-time stream scoring with batch retraining, with quantified drift analysis.

---

## Pre-Day Checklist (do this before Day 1 morning)

- [ ] Docker + Docker Compose installed, at least 8GB RAM allocated to Docker
- [ ] Python 3.10+ with venv, `pyflink`, `pyspark`, `kafka-python`, `xgboost`, `scikit-learn`, `pandas`, `delta-spark`, `streamlit` installed
- [ ] Dataset downloaded: **PaySim** (Kaggle: `ntnu-testimon/paysim1`) — chosen over IEEE-CIS because it's naturally sequential/time-ordered, which matters for realistic stream replay
- [ ] GitHub repo initialized with folder structure below
- [ ] Read PyFlink DataStream API docs skim (30 min) — specifically `KeyedStream`, `ProcessFunction`, and windowing, since this is your weakest area going in

**Repo structure**
```
fraud-lambda/
├── docker-compose.yml
├── data/                    # raw + processed datasets
├── producer/                # Kafka producer script
├── models/                  # trained model artifacts
├── notebooks/                # EDA, feature engineering, model training
├── flink-job/                # PyFlink streaming scorer
├── spark-batch/              # Spark retraining job
├── reconciliation/            # drift + comparison analysis
├── dashboard/                # Streamlit app
└── README.md
```

---

## DAY 1 — Environment Setup + EDA + Baseline Model

### Phase 1.1: Infrastructure (2–3 hrs)
- Write `docker-compose.yml` with services: Zookeeper, Kafka (single broker), Flink JobManager + TaskManager, Spark (standalone or use PySpark local mode instead — saves a service), Hive Metastore (with a Postgres backing DB).
- **Shortcut worth taking**: run Spark in local mode from your Python scripts rather than as a separate cluster service. One less thing to debug.
- Bring stack up, verify: Kafka topic can be created via CLI, Flink UI reachable at `localhost:8081`.
- **Time-box**: if Docker networking issues aren't resolved in 90 min, fall back to running Kafka+Zookeeper in Docker and Flink/Spark natively on host — don't lose a full day to infra.

### Phase 1.2: Data Understanding (1.5 hrs)
- Load PaySim, check class imbalance (fraud is typically <1% of transactions)
- Distribution checks: transaction amount by type, fraud rate by transaction type (PaySim fraud is concentrated in `TRANSFER`/`CASH_OUT`)
- Document 3–4 key findings in a notebook — this becomes README content later

### Phase 1.3: Feature Engineering (2–3 hrs)
Build features that a **stateful stream processor can realistically compute incrementally** (important — don't engineer features here that Flink can't maintain in state later):
- Rolling transaction count per user (last N minutes/txns)
- Rolling average transaction amount per user + deviation of current txn from it
- Time since user's last transaction
- Origin/destination balance consistency checks (PaySim-specific — flags where balances don't reconcile, a known strong fraud signal in this dataset)
- Merchant/transaction-type risk encoding

### Phase 1.4: Baseline Modeling (2 hrs)
- Train/test split respecting time order (no random shuffle — this is a temporal problem)
- Models: Logistic Regression (baseline) → XGBoost (primary)
- Use RepeatedStratifiedKFold + report AUC and PR-AUC (PR-AUC matters more here given imbalance)
- Export final model as `.pkl` (keep it simple — PyFlink UDF will load this directly; avoid ONNX unless you have time to spare, it adds a debugging surface)

**End of Day 1 deliverable**: trained model artifact + feature engineering logic documented as reusable Python functions (you'll call the same functions from both the Flink job and the Spark batch job — write them once, in a shared module).

---

## DAY 2 — Kafka Producer + Streaming Data Contract

### Phase 2.1: Schema Design (1 hr)
- Define transaction schema (JSON is fine — skip Avro/Schema Registry, it's not worth the setup time this week, but **mention in README that you'd use Avro + Schema Registry in production** for schema evolution guarantees)
- Fields: transaction_id, timestamp, user_id, amount, type, orig_balance_before/after, dest_balance_before/after

### Phase 2.2: Producer Script (2–3 hrs)
- Python script using `kafka-python` or `confluent-kafka`
- Replays PaySim rows **in timestamp order** with artificial delay (e.g., scale down PaySim's step-based time to real seconds) to simulate a live feed
- Add a `--speed-multiplier` flag so you can replay fast for testing, slow for demos

### Phase 2.3: Validation (1–2 hrs)
- Console consumer confirms messages flowing correctly
- Write a tiny consumer script that just counts messages/sec and checks schema — sanity check before Flink touches it
- Create the topic with sensible partition count (3 partitions is plenty for this scale — keyed by user_id if you want partition affinity, useful later for state locality)

**End of Day 2 deliverable**: Kafka topic receiving a realistic, time-ordered, replayable transaction stream.

---

## DAY 3–4 — Flink Speed Layer (hardest phase, budget 2 full days)

### Phase 3.1: PyFlink Job Skeleton (Day 3, morning)
- Kafka source connector → DataStream
- Deserialize JSON into a structured record type
- Get a trivial pass-through job running end-to-end first (source → print) before adding any logic — confirms plumbing works before you add complexity

### Phase 3.2: Stateful Feature Computation (Day 3, afternoon–evening)
- `keyBy(user_id)` to partition state per user
- `ProcessFunction` (or `KeyedProcessFunction`) maintaining:
  - Rolling window of recent transaction amounts/timestamps per user (ValueState/ListState)
  - Compute the same velocity/deviation features from Day 1, now incrementally
- **This is where most of your debugging time goes.** Common failure points: state not being cleared (TTL), event-time vs processing-time confusion, watermark issues. Use processing-time semantics for this project — event-time watermarking is a rabbit hole you don't need for a 1-week project (note this simplification explicitly in your README as a scoping decision).

### Phase 3.3: Model Scoring (Day 4, morning)
- Load the Day 1 pickled model inside the Flink job (as a Python UDF via PyFlink, or a `MapFunction`)
- Score each enriched record, threshold to flag high-risk transactions
- Output two sinks: (1) all scored transactions → Delta Lake table, (2) high-risk only → a Kafka "alerts" topic

### Phase 3.4: Delta Lake Sink (Day 4, afternoon)
- Write scored records to Delta Lake (via `delta-spark` if writing through a small Spark session as the sink, or write to Parquet + convert — simplest path: have Flink write to a staging location and a lightweight Spark structured streaming reader/writer commit it as Delta; don't over-engineer a direct Flink→Delta connector if time is short)
- Verify: query the Delta table, confirm time travel works (`DESCRIBE HISTORY`)

### Phase 3.5: Stabilize (Day 4, evening)
- Run the full pipeline (producer → Kafka → Flink → Delta + alerts) for at least 15–20 continuous minutes
- Fix any crashes/backpressure issues
- **Checkpoint deliverable**: live transactions scored in near-real-time, visible alerts, Delta table growing

---

## DAY 5 — Spark Batch Layer

### Phase 5.1: Simulate Delayed Labels (1 hr)
- Realistic framing: fraud labels in production arrive days after the transaction (confirmed by investigation). Simulate this by treating your "historical" data as fully labeled and the streaming data as initially unlabeled.

### Phase 5.2: Batch Feature Recomputation (2 hrs)
- Spark job reads full historical dataset (not the stream — the ground-truth batch source)
- Recompute the *same* feature logic from Day 1's shared module (reuse, don't rewrite — this consistency is something interviewers will explicitly probe: "how do you keep batch and streaming features consistent?")

### Phase 5.3: Retraining (1.5 hrs)
- Retrain XGBoost on the full historical + newly labeled data
- Compare new model's offline AUC/PR-AUC against Day 1's original model — save both versions (this pair is what you'll use in reconciliation)

### Phase 5.4: Hive + Iceberg Write (2 hrs)
- Write curated batch tables to **Hive** (via Hive Metastore, Parquet format) as your "ground truth" queryable layer
- **Stretch goal if time allows**: also write to **Iceberg** and do one small comparison — e.g., a schema evolution test (add a column, query old and new data) — enough to genuinely discuss the tradeoff, not a full benchmark

**End of Day 5 deliverable**: a retrained batch model + Hive tables representing the "corrected" ground-truth view.

---

## DAY 6 — Reconciliation & Drift Analysis (the data-science payoff)

### Phase 6.1: Join Speed vs Batch Predictions (1.5 hrs)
- For the same set of transactions, gather: (a) real-time score from the Flink/Day-1 model, (b) retrospective score from the Day-5 retrained batch model, (c) ground truth label (once available)

### Phase 6.2: Compute Comparison Metrics (2 hrs)
- Agreement rate between speed-layer and batch-layer flags
- AUC/PR-AUC of each model on the same holdout window
- **Drift quantification**: how does the streaming model's precision/recall degrade over the replay window, before batch retraining "refreshes" it? This is your headline number for the resume.
- Latency vs accuracy tradeoff: Flink's p50/p99 scoring latency vs the accuracy gap between speed and batch models

### Phase 6.3: Visualizations (1.5 hrs)
- Score distribution drift over time (histogram animation or small multiples)
- Precision/recall curve comparison, speed vs batch
- A simple timeline chart: alerts fired vs eventual confirmed fraud

**End of Day 6 deliverable**: a notebook with the drift/reconciliation numbers and charts — this is the section you'll quote directly in interviews.

---

## DAY 7 — Dashboard + Documentation + Polish

### Phase 7.1: Dashboard (2–3 hrs)
- Streamlit app (leverage your existing Plotly Dash experience) with:
  - Live/simulated alert feed
  - Drift chart from Day 6
  - Summary KPI cards (AUC, latency, agreement rate)

### Phase 7.2: Architecture Diagram (30 min)
- Clean diagram showing Kafka → Flink (speed) / Spark (batch) → Delta/Hive → reconciliation → dashboard

### Phase 7.3: README (2 hrs) — this matters as much as the code
Structure it as:
1. Problem framing (why Lambda architecture fits fraud detection)
2. Architecture diagram
3. Key design decisions **and their tradeoffs**, explicitly stated:
   - Why Flink over Spark Structured Streaming for the speed layer
   - Why Delta Lake as primary format, Iceberg evaluated for schema evolution
   - Why processing-time semantics instead of event-time (scoping decision under time constraint)
   - Why JSON schema instead of Avro+Schema Registry (same reason)
4. Results: your actual AUC numbers, latency numbers, drift %
5. "What I'd do with more time / at production scale" section (Schema Registry, event-time watermarking, exactly-once sinks, autoscaling Flink, feature store)

### Phase 7.4: Final End-to-End Demo Run (1–2 hrs)
- Run the full pipeline live once more, record a short screen capture (GIF or video) for your README/portfolio site — this is genuinely worth the time, since a working demo clip is far more convincing than a wall of text

**End of Day 7 deliverable**: a complete, documented, demoable repository ready to link from your resume.

---

## Resume Line (fill in real numbers from Day 6)

> *"Built a Lambda-architecture fraud detection system with real-time transaction scoring (Kafka, PyFlink stateful stream processing) and nightly batch retraining (Spark, Hive), landing curated data in Delta Lake; quantified a [X]% AUC/precision drift between retraining cycles and a [Y]ms p99 scoring latency, with Iceberg evaluated as an alternative table format."*

## Risk Notes (read before you start)
- Biggest time sink will be Flink state/windowing debugging (Day 3–4) — protect that time, don't let Day 1–2 run over.
- If Flink genuinely blocks you past Day 4 evening, the documented fallback is Spark Structured Streaming for the speed layer — cheaper to pivot late than to have no working speed layer at all.
- Keep the feature engineering logic in one shared Python module imported by both the Flink job and the Spark batch job from Day 1 onward — retrofitting this consistency later costs more time than building it in from the start.
