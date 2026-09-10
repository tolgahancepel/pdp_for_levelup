# Best Practices for Designing Data Pipelines
## Organized by Stage: Ingestion → Transformation → Load

---

## Cross-Cutting Principles (Apply to Every Stage)

Before the stage-specific breakdown, five principles anchor everything below:

1. **Idempotency by construction** — every stage should produce the same output if re-run with the same input (detailed patterns covered separately; referenced throughout).
2. **Immutability where possible** — prefer creating new data over mutating existing data; makes debugging and rollback tractable.
3. **Explicit over implicit** — schema, semantics, and contracts should be declared, not inferred.
4. **Fail loud, degrade deliberately** — decide consciously whether bad data should halt the pipeline or be quarantined; never let it fail silently.
5. **Design for reprocessing** — assume you *will* need to backfill or rerun; build every stage so that's a normal operation, not an emergency script.

---

## 1. Ingestion Best Practices

Ingestion is where external unpredictability enters your system. The goal: **capture data faithfully and safely, without letting source-side chaos propagate downstream.**

### 1.1 Land Raw Data Immutably (Bronze Layer)
- **Practice:** Write source data as-is, untransformed, into an append-only "raw" zone before any cleaning happens.
- **Why:** If a downstream bug is discovered weeks later, you can reprocess from raw data instead of re-extracting from a source that may have changed or purged records.
- **How:** Partition raw storage by ingestion date/batch id; never update or delete raw records — only add.

### 1.2 Extract Incrementally Using a Reliable Watermark
- **Practice:** Pull only new/changed data using a monotonic column (`updated_at`, auto-increment ID, CDC log position) rather than re-pulling entire datasets.
- **Why:** Full extracts don't scale and increase load on source systems; naive time-based windows can miss records if clocks drift or writes are delayed.
- **How:**
```sql
SELECT * FROM orders WHERE updated_at > :last_watermark
ORDER BY updated_at;
```
  Commit the new watermark **only after** the batch is successfully landed — never before (this is the idempotency anchor for extraction).

### 1.3 Validate Schema at the Point of Entry
- **Practice:** Check incoming data against an expected schema (types, required fields, value ranges) immediately upon ingestion.
- **Why:** Catching a broken schema at ingestion is cheap; catching it three transformation steps downstream means tracing back through multiple layers.
- **How:** Use `pydantic`/`pandera` for batch, or a schema registry (Confluent/Glue) with compatibility rules for streaming.

### 1.4 Capture Ingestion Metadata
- **Practice:** Attach `ingestion_timestamp`, `source_system`, `batch_id`/`file_name`, and (for CDC) `lsn`/offset to every ingested record.
- **Why:** This metadata is what makes deduplication, lineage tracing, and idempotent reprocessing possible later — it's much harder to retrofit after the fact.

### 1.5 Handle Source System Impact Deliberately
- **Practice:** Rate-limit API extraction, use read replicas for DB extraction (not production primaries), and paginate large pulls.
- **Why:** A poorly designed extraction job can degrade the performance of the source production system — a very common real-world incident.

### 1.6 Isolate Malformed Records, Don't Block the Batch
- **Practice:** Route records that fail schema/format validation to a dead-letter location; let valid records continue.
- **Why:** One malformed row shouldn't block ingestion of the other 999,999 valid rows.
- **How:** Log the raw payload + validation error to a `_rejected` table/topic for later inspection and reprocessing.

### 1.7 Choose the Right Extraction Pattern for the Source
| Pattern | When to Use | Trade-off |
|---|---|---|
| **Full extract** | Small reference/dimension tables | Simple, but doesn't scale |
| **Incremental (watermark)** | Large tables with an `updated_at` column | Misses hard deletes unless paired with a reconciliation job |
| **CDC (log-based)** | High-volume transactional tables, need low latency | More infrastructure (Debezium, connectors), but captures deletes and full change history |

---

## 2. Transformation Best Practices

Transformation is where business logic lives — and where subtle correctness bugs are most likely to hide.

### 2.1 Write Pure, Deterministic Transformations
- **Practice:** Given the same input data, a transformation must always produce the same output. Avoid `NOW()`, unseeded random sampling, or reliance on external mutable lookups mid-transformation.
- **Why:** This is the foundation of idempotent, testable pipelines — non-determinism here breaks every downstream guarantee.
- **How:** If a transformation needs "current time," pass it in explicitly as a parameter (e.g., the DAG's execution date) rather than calling a live clock function inside the logic.

### 2.2 Modularize Transformations into Small, Testable Units
- **Practice:** Break transformation logic into discrete, single-purpose steps (staging → intermediate → mart), each independently testable — mirrors dbt's staging/intermediate/mart convention.
- **Why:** A monolithic 500-line SQL query or Spark job is nearly impossible to debug or unit test; small composable steps isolate failure points.

### 2.3 Embed Data Quality Checks as Part of the Pipeline, Not an Afterthought
- **Practice:** Add explicit assertions after key transformation steps — row count deltas, null-rate thresholds, referential integrity, uniqueness of keys.
- **Why:** A join that unexpectedly duplicates rows, or a filter that drops 90% of records, should fail the pipeline (or alert), not silently ship bad data to a dashboard.
- **How:**
```python
assert output_df.count() == input_df.count(), "Unexpected row multiplication in join"
assert output_df.filter(col("customer_id").isNull()).count() == 0, "Unexpected nulls in key column"
```
  Tools: dbt tests, Great Expectations, Deequ.

### 2.4 Handle Late-Arriving & Out-of-Order Data Explicitly
- **Practice:** Define a lateness tolerance/watermark for streaming aggregations; for batch, define whether a partition is "final" after N hours or reopened on late data arrival.
- **Why:** Silent gaps from late data are a top source of "the numbers don't match" incidents — must be a conscious design decision, not a default behavior.

### 2.5 Manage Data Skew and Join Cardinality Proactively
- **Practice:** Profile key distributions before designing joins; use broadcast joins for small skewed dimensions, salting for skewed large-key joins.
- **Why:** Prevents jobs that work in dev/sample data from failing at production scale.

### 2.6 Keep Transformation Logic in Version-Controlled, Testable Code
- **Practice:** SQL models (dbt), Spark jobs, or Python transformations should live in git, go through code review, and have unit tests with fixed fixtures.
- **Why:** Enables safe iteration, rollback, and traceability of *which logic version* produced *which historical output* — critical for debugging data discrepancies discovered weeks later.

### 2.7 Design Idempotent Transformation Outputs
- **Practice:** Ensure re-running a transformation for the same logical batch (same execution date/partition) produces byte-for-byte the same result.
- **Why:** This is what makes backfills and retries safe — the transformation stage's contribution to overall pipeline idempotency.
- **How:** Deterministic surrogate keys (hash of natural key), no non-deterministic ordering-dependent logic (e.g., `LIMIT` without `ORDER BY`).

---

## 3. Load Best Practices

Load is where correctness guarantees must actually be enforced in storage — this is the layer most directly responsible for avoiding duplication and corruption.

### 3.1 Load via MERGE/Upsert or Partition Overwrite — Never Blind Append
- **Practice:** Choose one of two patterns based on data shape:
  - **MERGE on natural key** — for incremental/CDC-style loads where rows update in place.
  - **Partition overwrite** — for batch loads bucketed by a reprocessing key (date, batch_id).
- **Why:** Blind appends require a separate, error-prone dedup step downstream; MERGE/overwrite make idempotency the default behavior of the write itself.
- *(Full implementation patterns covered in the idempotency deep-dive — this is the load-stage summary of that principle.)*

### 3.2 Use a Staging-Then-Swap Pattern for Atomicity
- **Practice:** Write to a staging table/location first, validate, then atomically swap/rename into production (or do the MERGE from staging).
- **Why:** Prevents partial writes from leaving the production table in an inconsistent state if the job fails mid-write.
```python
load_to_staging(df)           # safe to retry: staging is fully overwritten each run
validate(staging_table)       # row counts, schema checks
atomic_merge(staging_table, production_table)
```

### 3.3 Commit Watermarks/Checkpoints Only After a Successful Load
- **Practice:** Update the extraction watermark or streaming checkpoint offset in the same transaction as (or strictly after) the successful write to the target.
- **Why:** If the watermark advances before the write is confirmed, a crash creates a permanent, silent data gap.

### 3.4 Design the Target Partitioning and File Layout for Query Patterns
- **Practice:** Partition by the column most frequently filtered on (usually date); avoid high-cardinality partition keys; target file sizes in the 128MB–1GB range.
- **Why:** Directly determines query cost/latency downstream; poor layout causes full-table scans and small-file overhead.

### 3.5 Handle Compaction for Streaming/Frequent Loads
- **Practice:** If loading in small frequent batches (streaming/micro-batch), schedule periodic compaction of small files, or use a table format with auto-compaction (Delta, Iceberg, Hudi).
- **Why:** Left unmanaged, small files silently degrade read performance over time until someone investigates why "the dashboard got slow."

### 3.6 Choose the Right Slowly Changing Dimension (SCD) Strategy
- **Practice:** For dimension tables, decide explicitly: overwrite (Type 1 — no history), or version with `valid_from`/`valid_to` (Type 2 — full history).
- **Why:** This is a common design gap — teams often default to Type 1 without realizing they've lost the ability to answer "what did this record look like at time X."
```sql
-- Type 2 pattern: close old record, insert new version
UPDATE dim_customer SET valid_to = current_date, is_current = false
WHERE customer_id = :id AND is_current = true;

INSERT INTO dim_customer (customer_id, attributes, valid_from, valid_to, is_current)
VALUES (:id, :new_attributes, current_date, NULL, true);
```

### 3.7 Version the Target Schema and Communicate Breaking Changes
- **Practice:** When a load target's structure changes in a breaking way, version it (`orders_v2`) or maintain a compatibility view rather than mutating the existing table in place.
- **Why:** Downstream consumers (BI dashboards, other pipelines) depend on stable structure — silent breaking changes cascade failures across teams.

### 3.8 Validate Post-Load, Not Just Pre-Load
- **Practice:** After the write completes, run a lightweight reconciliation check — row counts match staging, key uniqueness holds, no unexpected nulls in critical columns.
- **Why:** Catches issues introduced by the write mechanism itself (partial merge, truncation bugs) that pre-load validation can't see.

---

## Summary Table: Best Practice by Stage

| Stage | Core Guarantee to Design For | Primary Mechanism |
|---|---|---|
| **Ingestion** | Faithful, safe capture of source data | Immutable raw landing + watermark-based incremental pull + schema validation at entry |
| **Transformation** | Deterministic, correct business logic | Pure functions + modular testable steps + embedded data quality assertions |
| **Load** | No duplication/corruption on retry | MERGE/overwrite + staging-swap pattern + checkpoint-after-write ordering |

---

## Practical Exercise
For your next pipeline design, walk through each stage and answer:
1. **Ingestion:** What's my watermark, and what happens if I extract the same window twice?
2. **Transformation:** If I rerun this exact transformation on the exact same input, is the output byte-identical?
3. **Load:** If this write fails at 60% completion and retries, what's the state of the target table?

If you can't answer all three concretely for a given pipeline, that's the design gap to close before shipping it.