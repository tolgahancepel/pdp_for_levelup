# Delta Live Tables (DLT): Comprehensive Study Guide

## 1. Conceptual Foundation

### 1.1 What DLT Actually Is
Delta Live Tables is a **declarative framework** built on top of Apache Spark and Delta Lake for building data pipelines. The critical mental shift from traditional ETL:

- **Traditional approach**: You write *imperative* code that says "read this, transform it this way, write it there, then do this next"
- **DLT approach**: You *declare* what each table should contain, and DLT figures out the execution order, dependency graph, and orchestration

Think of it as the difference between writing SQL queries versus defining views — DLT tables are more like materialized view definitions where DLT manages the *how* and *when*.

### 1.2 The Core Abstraction: Dataflow Graph
When you define multiple DLT tables, DLT automatically constructs a **Directed Acyclic Graph (DAG)** based on table dependencies (inferred from your SQL/Python code referencing other tables). You never manually specify "run this after that" — the framework infers it.

```python
import dlt

@dlt.table
def bronze_orders():
    return spark.readStream.table("raw.orders")

@dlt.table
def silver_orders():
    # DLT knows this depends on bronze_orders because of this reference
    return dlt.read("bronze_orders").filter("status != 'cancelled'")
```

---

## 2. The Medallion Architecture (Bronze/Silver/Gold) in DLT

This isn't unique to DLT, but DLT is *purpose-built* to implement it cleanly.

| Layer | Purpose | Typical Operations |
|-------|---------|---------------------|
| **Bronze** | Raw ingestion, preserve source fidelity | Minimal transformation, schema-on-read, add ingestion metadata |
| **Silver** | Cleaned, conformed, deduplicated | Type casting, dedup, joins, quality enforcement |
| **Gold** | Business-level aggregates | Aggregations, dimensional models, metrics |

**Real-world use case**: A retail company ingests clickstream events (bronze), deduplicates and joins with customer master data (silver), then produces daily active user metrics per region (gold). Each layer is a DLT table, and a failure in bronze automatically halts downstream silver/gold refresh — preventing garbage propagation.

---

## 3. Streaming Tables vs. Materialized Views (Live Tables)

This is one of the most misunderstood distinctions.

### 3.1 Streaming Tables (`dlt.table` + `spark.readStream`)
- Process data **incrementally** — only new data since last run
- Backed by Structured Streaming under the hood
- Append-only source assumption (unless using CDC patterns)
- Use when: source is continuously growing (Kafka, autoloader files, append-only logs)

### 3.2 Materialized Views (`dlt.table` + `spark.read`, batch)
- **Recomputed fully** on every pipeline run (or incrementally if DLT can optimize it)
- Use when: you need full recalculation logic (e.g., aggregations that must reconsider all historical data, slowly changing dims)

**Key real-world decision point**: 
> If you're computing "total revenue per customer" and revenue rows can be *updated* (not just appended), a naive streaming table will double-count. You need either a materialized view or CDC-aware processing (see APPLY CHANGES INTO below).

---

## 4. Change Data Capture: `APPLY CHANGES INTO`

This is DLT's flagship feature for solving a historically painful problem: **applying upserts/deletes from CDC feeds without writing manual MERGE logic.**

### The Problem It Solves
Traditionally, handling CDC (e.g., from Debezium, Fivetran, or database logs) required hand-written `MERGE INTO` statements, careful handling of out-of-order events, and manual SCD (Slowly Changing Dimension) logic.

### The DLT Solution
```python
dlt.create_streaming_table("customers_silver")

dlt.apply_changes(
    target="customers_silver",
    source="customers_cdc_bronze",
    keys=["customer_id"],
    sequence_by="operation_timestamp",  # determines "latest" record
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["operation", "operation_timestamp"],
    stored_as_scd_type=1  # or 2
)
```

### SCD Type 1 vs Type 2 — Real Use Case
- **Type 1** (overwrite): Customer updates their address → old address is gone. Used for operational dashboards where only current state matters.
- **Type 2** (history preserved): Insurance company needs to know a customer's address *at the time* a claim was filed → DLT automatically adds `__START_AT` / `__END_AT` columns and preserves row history.

**Why this matters in interviews/real work**: This single feature can replace hundreds of lines of custom MERGE + deduplication + late-arriving-data handling code.

---

## 5. Data Quality: Expectations

DLT bakes data quality directly into the pipeline definition rather than as a separate validation step.

```python
@dlt.table
@dlt.expect_or_drop("valid_order_amount", "order_amount > 0")
@dlt.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("suspicious_quantity", "quantity < 10000")  # just tracks, doesn't block
def silver_orders():
    return dlt.read_stream("bronze_orders")
```

| Directive | Behavior | Use Case |
|-----------|----------|----------|
| `expect` | Log violation, keep row | Monitoring soft business rules |
| `expect_or_drop` | Log + drop violating rows | Filtering known bad data (nulls, negative amounts) |
| `expect_or_fail` | Log + **halt pipeline** | Critical invariants (e.g., primary key must never be null) |

**Real-world use case**: A financial institution needs SOX-compliant lineage — every row that fails quality checks is quantified in pipeline event logs (queryable via `event_log()` system table), giving auditors a full trail of *what* was dropped and *why*, without needing a separate quality framework like Great Expectations bolted on.

**Nuance**: Expectations don't stop the whole pipeline unless `expect_or_fail` is violated — this lets you build resilient pipelines that quarantine bad data rather than crashing on every anomaly.

---

## 6. Pipeline Execution Modes

### 6.1 Triggered vs. Continuous
- **Triggered**: Runs once, processes available data, shuts down. Good for scheduled batch-like workloads (cost-efficient).
- **Continuous**: Pipeline stays alive, processing micro-batches continuously (low-latency streaming use cases, e.g., fraud detection needing sub-minute freshness).

### 6.2 Development vs. Production Mode
- **Development**: Reuses cluster across runs, faster iteration, less resilient to failures (no auto-retry)
- **Production**: Auto-retries transient failures, terminates cluster after each run (cost control), used for scheduled jobs

**Real-world tip**: Always develop in Dev mode to avoid cold-start Spark cluster overhead (minutes) on every code change; switch to Production before scheduling.

---

## 7. Incremental Processing Internals

### 7.1 Why Incremental Matters
Without incremental processing, every pipeline run reprocesses your *entire* dataset — untenable at scale (imagine reprocessing 5 years of clickstream data daily).

### 7.2 How DLT Achieves It
- Streaming tables use Structured Streaming's checkpointing to track "what's already been processed"
- DLT manages checkpoints automatically (you never see `checkpointLocation` — this is abstracted away, unlike raw Structured Streaming)

### 7.3 The Autoloader Pairing
DLT is commonly paired with **Autoloader** (`cloudFiles` format) for cloud file ingestion:

```python
@dlt.table
def bronze_events():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/schemas/events")
        .load("/mnt/raw/events")
    )
```

**Real-world use case**: A company receives millions of small JSON files per day in S3. Autoloader + DLT incrementally discovers new files (via file notification or directory listing) without re-scanning the entire bucket — critical for cost and latency at scale.

---

## 8. Schema Evolution & Enforcement

DLT tables are Delta tables underneath, so they inherit Delta's schema enforcement, but DLT adds pipeline-level handling:

- **`schema evolution`** at the Autoloader level (`cloudFiles.schemaEvolutionMode`) handles new columns arriving in source data
- Pipeline can be configured to fail or adapt when schema drift occurs
- Real use case: An upstream team adds a new field `promo_code` to the order events without notice. With schema evolution set to `addNewColumns`, the pipeline adapts automatically instead of crashing at 3 AM.

---

## 9. Orchestration & Dependency Management

### 9.1 Why This Matters Operationally
In traditional orchestration (Airflow), you manually define task dependencies — a source of bugs when someone changes a transformation but forgets to update the DAG.

In DLT: **dependencies are self-documenting** because they're derived from the code itself (`dlt.read("table_name")` calls). This eliminates an entire class of orchestration bugs — "silent DAG drift."

### 9.2 Pipeline as a Single Deployable Unit
A DLT pipeline is defined by a set of notebooks/files + a JSON configuration (target catalog/schema, cluster policy, etc.). This whole unit can be:
- Version controlled (Git)
- Deployed via Databricks Asset Bundles / Terraform
- Parameterized per environment (dev/staging/prod) using pipeline configuration values

---

## 10. Monitoring & Observability

### 10.1 Event Log
Every DLT pipeline writes a queryable **event log** (a Delta table) capturing:
- Data quality metric results (rows passed/dropped/failed per expectation)
- Pipeline run lineage
- Cluster and performance metrics

```sql
SELECT expectations
FROM event_log(<pipeline_id>)
WHERE event_type = 'flow_progress'
```

**Real-world use case**: SLA reporting — "what % of rows failed quality checks this week per table" becomes a simple query against the event log, feeding a Grafana/Databricks SQL dashboard for data quality trending.

### 10.2 Lineage
DLT pipelines automatically populate **Unity Catalog lineage** — you can trace a gold table's column back through every silver/bronze transformation without manual documentation. Critical for impact analysis ("if I change this bronze schema, what breaks downstream?").

---

## 11. Key Trade-offs & When *Not* to Use DLT

Balanced perspective — this matters for real decision-making:

| Scenario | DLT Fit |
|----------|---------|
| Multi-hop batch/streaming pipelines with clear lineage needs | ✅ Excellent fit |
| Need for CDC/upsert handling | ✅ `apply_changes` is best-in-class |
| Highly complex custom orchestration (e.g., calling external APIs mid-pipeline, conditional branching across unrelated systems) | ⚠️ Consider Airflow/Workflows orchestrating DLT as one step |
| Extremely small one-off scripts / ad hoc analysis | ❌ Overhead not justified |
| Need fine-grained control over Spark checkpoint locations, custom trigger intervals per stream | ⚠️ Limited compared to raw Structured Streaming |

---

## 12. Putting It Together: End-to-End Real-World Scenario

**Scenario**: E-commerce platform needs near-real-time inventory accuracy across warehouses.

1. **Bronze**: Autoloader ingests raw inventory change events (JSON) from Kafka sink files → `bronze_inventory_events` streaming table, schema evolution enabled for new warehouse-specific fields.
2. **Silver**: `apply_changes` with `sequence_by="event_timestamp"`, `keys=["sku", "warehouse_id"]`, SCD Type 1 → gives current inventory state per SKU/warehouse, handling out-of-order Kafka delivery automatically.
3. **Expectations**: `expect_or_drop("valid_quantity", "quantity >= 0")` — negative inventory is impossible, drop and log for investigation.
4. **Gold**: Materialized view aggregating `silver_inventory` by region for the executive dashboard, recomputed each triggered run.
5. **Orchestration**: Pipeline runs on 15-minute trigger schedule (triggered mode, not continuous — 15 min freshness is acceptable, saves cluster costs vs always-on).
6. **Monitoring**: Data quality dashboard built on `event_log()` alerts the inventory team if dropped-row rate exceeds 1% (indicating upstream sensor malfunction).

This single scenario touches nearly every concept above — that integration is the real skill being tested in interviews and production work.