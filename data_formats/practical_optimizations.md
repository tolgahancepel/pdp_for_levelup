# Practical Guide: Parquet & Delta — Examples, Common Issues, Fixes

---

## PART 1: PARQUET — Practical Examples

### Example 1: Inspecting Actual File Structure

Don't guess — inspect. This is the single most useful habit.

```python
import pyarrow.parquet as pq

pf = pq.ParquetFile("part-0001.parquet")

print(pf.metadata.num_row_groups)        # how many row groups?
print(pf.metadata.num_columns)
print(pf.schema)

# Per row-group, per-column stats
rg = pf.metadata.row_group(0)
for i in range(rg.num_columns):
    col = rg.column(i)
    print(col.path_in_schema, col.total_compressed_size,
          col.statistics.min, col.statistics.max, col.statistics.null_count)
```

Or from the shell:
```bash
parquet-tools meta part-0001.parquet
parquet-tools rowcount part-0001.parquet
```

**Use this to answer:** Are my row groups the size I think they are? Are stats present (min/max)? Is dictionary encoding kicking in?

---

### Example 2: Predicate Pushdown Failing Silently

**Setup:** table partitioned by `ingestion_date`, filtered by `event_date`.

```python
df.write.partitionBy("ingestion_date").parquet("/data/events")

spark.read.parquet("/data/events").filter("event_date = '2024-01-01'").explain()
```

**Symptom:** `PushedFilters: [IsNotNull(event_date), EqualTo(event_date,...)]` shows up in the plan (good), but query still scans everything.

**Root cause:** `event_date` isn't correlated with `ingestion_date` (the physical write order) — every row group has a min/max for `event_date` spanning the whole range, because rows were written in ingestion order, not event order. Stats-based row-group pruning has nothing to prune.

**Fix:** Sort within each write, or partition/cluster by the actual filter column:

```python
df.sort("event_date").write.partitionBy("ingestion_date").parquet("/data/events")
```
Or in Delta, use **Z-ORDER** (see Delta section) — same underlying principle.

**Verify the fix worked:**
```python
spark.read.parquet("/data/events").filter("event_date = '2024-01-01'") \
    .explain("formatted")
# check "number of files read" / bytes scanned in the Spark UI, before vs after
```

---

### Example 3: Row Group Size Mismatch from Small Files

**Symptom:** 10,000 files, each 2MB, each with 1 row group. Queries are slow despite small total data volume (20GB).

**Diagnosis:**
```python
pf = pq.ParquetFile("part-0001.parquet")
print(pf.metadata.num_row_groups)          # 1
print(pf.metadata.row_group(0).total_byte_size)  # ~2MB
```
Each file open = one S3 GET + footer parse + row group read. 10,000 files = 10,000 round trips, regardless of total volume.

**Fix — compact before reading heavily:**
```python
spark.conf.set("spark.sql.files.maxRecordsPerFile", 5_000_000)

(spark.read.parquet("/data/events")
    .repartition(50)                      # fewer, bigger output files
    .write.mode("overwrite").parquet("/data/events_compacted"))
```
Target: 128MB–512MB per file for object storage.

---

### Example 4: Dictionary Encoding Blowing Up File Size

**Symptom:** A `customer_id` (UUID string, high cardinality) column makes the file bigger than expected, and write is slow.

**Cause:** Parquet writers try dictionary encoding first; if a column chunk's dictionary exceeds a size threshold (default ~1MB in most writers), it **falls back to plain encoding mid-chunk** — you pay the cost of building the dictionary AND get no benefit.

**Diagnosis:**
```python
col = pf.metadata.row_group(0).column(idx_of_customer_id)
print(col.encodings)   # look for PLAIN_DICTIONARY vs PLAIN fallback
```

**Fix:** Disable dictionary encoding for known-high-cardinality columns, or reorder so low-cardinality columns benefit while high-cardinality ones don't waste dictionary-build overhead:
```python
df.write.option("parquet.enable.dictionary", "false").parquet(...)
# or per-column via writer-specific configs where supported
```

---

### Example 5: Wide Tables (1000+ columns) — Footer & Metadata Overhead

**Symptom:** Reading even a single column from a very wide table is slow; file open is slow.

**Cause:** Each column has its own chunk metadata (offsets, stats, encodings) in the footer. With 1000+ columns × many row groups, the **footer itself becomes large** (sometimes tens of MB), and it must be fully parsed before any data read.

**Fix:**
- Reduce row group count (fewer, larger row groups → less repeated per-row-group metadata).
- Consider splitting extremely wide tables into logical column groups across separate tables/files if only subsets are queried together.
- Increase row group size explicitly:
```python
spark.conf.set("parquet.block.size", 256 * 1024 * 1024)  # 256MB row groups
```

---

## PART 2: DELTA LAKE — Practical Examples

### Example 1: Reproducing (and Diagnosing) the Classic Storage Growth Problem

```sql
CREATE TABLE t (id INT, val STRING) USING DELTA;
INSERT INTO t SELECT id, 'v0' FROM range(1000000);

-- Simulate 100 daily updates touching 1% of rows each
UPDATE t SET val = 'v1' WHERE id % 100 = 0;
UPDATE t SET val = 'v2' WHERE id % 100 = 1;
-- ... repeated over time
```

**Check logical vs physical size:**
```sql
DESCRIBE DETAIL t;
-- sizeInBytes here = size of CURRENT active files only
```

```python
import subprocess
subprocess.run(["du", "-sh", "/path/to/t"])  # actual disk usage — will be much bigger
```

**Why the gap:** each `UPDATE` rewrote whole files containing matched rows and tombstoned the originals. Old files are still physically present.

**Diagnose the tombstones:**
```sql
DESCRIBE HISTORY t;   -- see operationMetrics: numRemovedFiles, numAddedFiles per commit
```

```python
# Look at raw log to see remove actions accumulating
for f in sorted(os.listdir("/path/to/t/_delta_log"))[-5:]:
    print(f)
```

**Fix:**
```sql
VACUUM t RETAIN 168 HOURS;   -- default 7 days; physically deletes eligible tombstoned files
VACUUM t RETAIN 168 HOURS DRY RUN;  -- always dry-run first in prod, for review
```

**Guardrail — don't do this in prod without checking:**
```sql
SET spark.databricks.delta.retentionDurationCheck.enabled = true; -- keep this ON
-- forcing a shorter retention than 7 days can break time travel / concurrent long-running readers
```

---

### Example 2: Small-File Accumulation from Streaming Writes

```python
(stream_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/chk")
    .trigger(processingTime="30 seconds")
    .start("/data/events_delta"))
```

**Symptom:** After a few days, thousands of tiny files, and `DESCRIBE HISTORY` shows hundreds of commits, each with `numAddedFiles: 1-2`.

**Diagnosis:**
```sql
SELECT count(*) FROM (
  SELECT input_file_name() AS f FROM events_delta
) GROUP BY f -- or just check DESCRIBE DETAIL numFiles vs sizeInBytes
```
```sql
DESCRIBE DETAIL events_delta;
-- numFiles: 50,000  sizeInBytes: 20GB  → avg file size = 400KB (bad)
```

**Fix — enable auto-compaction and optimized writes going forward:**
```python
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
```

**Fix existing backlog:**
```sql
OPTIMIZE events_delta;                          -- bin-packs small files into larger ones
OPTIMIZE events_delta ZORDER BY (event_date);    -- also improves pruning if filtered on event_date
```

**Follow-up (mandatory):**
```sql
VACUUM events_delta;  -- OPTIMIZE creates new files + tombstones old small ones —
                       -- storage won't shrink until this runs
```

---

### Example 3: Z-ORDER Not Improving Query Performance

```sql
OPTIMIZE t ZORDER BY (customer_id);
```

**Symptom:** No measurable improvement in filtered query time.

**Common causes and fixes:**

| Cause | Check | Fix |
|---|---|---|
| Column already low-cardinality / boolean | `SELECT approx_count_distinct(col) FROM t` | Z-order on a higher-cardinality, actually-filtered column |
| Table already partitioned by a correlated column, leaving little to optimize | Check partition scheme | Z-order within partitions is still useful, but expectations should be tempered |
| Z-ORDER run before data was fully compacted (still many small files) | `DESCRIBE DETAIL` | Run plain `OPTIMIZE` first, then `ZORDER` |
| Query doesn't actually filter on the Z-ordered column | Check query predicate | Z-order matches your actual `WHERE` clause columns |
| Data changed significantly since last OPTIMIZE (new writes not clustered) | `DESCRIBE HISTORY` for recent appends without OPTIMIZE | Re-run OPTIMIZE periodically, not once |

**Verify improvement:**
```sql
EXPLAIN SELECT * FROM t WHERE customer_id = 'X';
-- check "PartitionFilters"/"files read" in the Spark UI's SQL tab, before/after
```

---

### Example 4: Concurrent Write Conflicts

```python
# Two jobs running simultaneously
# Job A:
spark.sql("UPDATE t SET val = 'a' WHERE region = 'US'")
# Job B (concurrently):
spark.sql("UPDATE t SET val = 'b' WHERE region = 'EU'")
```

**Symptom:**
```
ConcurrentAppendException: Files were added to the table by a concurrent update...
```

**Cause:** Delta uses optimistic concurrency control. Both transactions read the same starting version; if their read/write sets conflict at the file level (not always logically necessary — file-level granularity), one loses and must retry.

**Fix 1 — partition the table by the column used to segregate concurrent writers:**
```sql
CREATE TABLE t (id INT, region STRING, val STRING)
USING DELTA
PARTITIONED BY (region);
```
Now Job A and Job B touch disjoint partitions/files → no conflict.

**Fix 2 — let Delta retry automatically (it does, up to a limit) but ensure idempotency:**
```python
spark.conf.set("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true")
```
For genuinely necessary concurrent conflicting writes, restructure into a single writer + queue, or use `MERGE` with condition narrowing to reduce overlap.

---

### Example 5: Time Travel Fails Unexpectedly

```sql
SELECT * FROM t VERSION AS OF 12;
```
```
Error: [DELTA_TIME_TRAVEL_INVALID_BEGIN_VALUE] ... 
or files referenced by version 12 no longer exist
```

**Cause:** `VACUUM` already ran and physically deleted files that version 12 depends on. Time travel range is bounded by whichever came first: log retention (`delta.logRetentionDuration`, default 30 days) or file retention via VACUUM (default 7 days).

**Diagnosis:**
```sql
DESCRIBE HISTORY t; -- check timestamps of VACUUM operations vs the version you want
```

**Fix (going forward) — align retention with actual time-travel requirements:**
```sql
ALTER TABLE t SET TBLPROPERTIES (
  'delta.logRetentionDuration' = '30 days',
  'delta.deletedFileRetentionDuration' = '30 days'  -- match your longest time-travel need
);
```
Note: this increases storage retained between VACUUMs — it's a direct storage-vs-time-travel tradeoff, not free.

---

### Example 6: MERGE Causing Disproportionate File Rewrites

```sql
MERGE INTO target t
USING updates u ON t.id = u.id
WHEN MATCHED THEN UPDATE SET t.val = u.val
WHEN NOT MATCHED THEN INSERT *;
```

**Symptom:** Updating 0.1% of rows rewrites 40% of the table's files, and `DESCRIBE HISTORY` shows huge `numAddedBytes`/`numRemovedBytes` relative to actual changed row count.

**Cause:** Without deletion vectors, MERGE must rewrite any *entire file* that contains at least one matched row. If matched rows are scattered across many files (poor clustering on the merge key), most files get touched.

**Diagnosis:**
```sql
DESCRIBE HISTORY t;
-- look at operationMetrics: numTargetRowsUpdated vs numTargetFilesAdded/numTargetFilesRemoved
```

**Fix 1 — enable Deletion Vectors (avoids full-file rewrite for deletes/some updates):**
```sql
ALTER TABLE t SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);
```

**Fix 2 — cluster the table on the merge key so matched rows concentrate in fewer files:**
```sql
OPTIMIZE t ZORDER BY (id);
```

**Fix 3 — reduce file size target so fewer rows get "collaterally rewritten" per touched file:**
```sql
ALTER TABLE t SET TBLPROPERTIES ('delta.targetFileSize' = '32mb');
```
(Smaller files = less rewritten per matched row, but more files overall — a real tradeoff against the small-file problem in Example 2.)

---

### Example 7: Slow Table Load Due to Log Replay

**Symptom:** Simply opening the table (`spark.read.format("delta").load(...)`) takes 10+ seconds even before any query runs.

**Diagnosis:**
```python
import os
print(len(os.listdir("/data/t/_delta_log")))  # thousands of .json files?
```
```sql
DESCRIBE HISTORY t LIMIT 1; -- check current version number
```
If version is in the thousands and you don't see recent `.checkpoint.parquet` files near the latest version, checkpoints aren't keeping up (or were disabled).

**Fix:**
```sql
-- Force a checkpoint now
CALL delta.checkpoint('t');   -- or in older versions, trigger via a no-op write
```
```python
spark.conf.set("spark.databricks.delta.checkpointInterval", "10")  # default; verify not overridden higher
```
Confirm `_last_checkpoint` file exists and is recent:
```bash
cat /data/t/_delta_log/_last_checkpoint
```

---

### Example 8: Schema Evolution Gone Wrong

```python
df_with_new_col.write.format("delta").mode("append").save("/data/t")
```
```
AnalysisException: A schema mismatch detected when writing to the Delta table...
```

**Fix (intentional additive evolution):**
```python
df_with_new_col.write.format("delta").mode("append") \
    .option("mergeSchema", "true").save("/data/t")
```

**For destructive/type changes (use carefully):**
```python
df_new_schema.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").save("/data/t")
# WARNING: this changes schema for ALL versions going forward;
# combined with prior data via time travel can produce confusing read errors
# on old versions with the old schema — expected but often surprising.
```

---

## PART 3: Consolidated Troubleshooting Table

| Symptom | Root Cause | Diagnostic Command | Fix |
|---|---|---|---|
| Physical size >> `DESCRIBE DETAIL` size | Un-vacuumed tombstones | `DESCRIBE HISTORY`, `du -sh` | `VACUUM` |
| Many small files, slow reads | Frequent low-volume commits | `DESCRIBE DETAIL` (numFiles vs sizeInBytes) | `OPTIMIZE` + enable autoCompact/optimizeWrite |
| Filter query scans everything | Data not clustered on filter column | `EXPLAIN`, check min/max stats per file | Sort on write, or `OPTIMIZE ZORDER BY` |
| Z-ORDER shows no improvement | Wrong column, or table not compacted first | `approx_count_distinct`, `DESCRIBE HISTORY` | Plain `OPTIMIZE` first, Z-order on actually-filtered high-cardinality column |
| `ConcurrentAppendException` | Optimistic concurrency conflict at file level | Job logs / stack trace | Partition by writer-segregating column; retry logic |
| Time travel fails for old version | VACUUM/log retention already expired that version | `DESCRIBE HISTORY` | Increase retention settings *before* it's needed |
| MERGE rewrites far more than matched rows | Matched rows scattered across many files | `operationMetrics` in `DESCRIBE HISTORY` | Deletion vectors, `ZORDER` on merge key, smaller target file size |
| Slow table open before any query runs | Checkpoint lag, too many JSON commits since last checkpoint | Count `_delta_log` files, check `_last_checkpoint` | Force checkpoint, verify checkpoint interval |
| Schema mismatch on write | Unintended additive/breaking schema drift | Compare `df.schema` vs `DESCRIBE t` | `mergeSchema` (additive) or `overwriteSchema` (breaking, deliberate) |
| Parquet file size ballooning for a column | Dictionary fallback due to high cardinality | `parquet-tools meta`, check encodings | Disable dictionary encoding for that column |
| Wide-table reads slow even for 1 column | Large footer from many columns × row groups | `pyarrow` metadata size check | Increase row group size (fewer row groups), reduce footer overhead |

---

## Quick Operational Checklist (put this in a runbook)

1. **After every OPTIMIZE/heavy UPDATE/DELETE/MERGE cycle** → schedule `VACUUM` (with `DRY RUN` first in prod).
2. **For streaming/frequent-write tables** → enable `autoCompact` + `optimizeWrite` from day one, don't wait for the small-file problem to appear.
3. **Before running `OPTIMIZE ZORDER`** → confirm it targets columns actually used in `WHERE` clauses, and re-run periodically (it's not "set once").
4. **Before shortening retention settings** → confirm no downstream job/dashboard relies on time travel beyond that window.
5. **For MERGE-heavy workloads on large tables** → evaluate deletion vectors early; it changes the storage-churn math fundamentally.
6. **Periodically check `_delta_log` size and checkpoint recency**, especially on tables with thousands of commits — this is invisible until someone complains about "table just opening slowly."

Want me to build out a specific one of these into a **runnable end-to-end demo notebook** (e.g., simulate the storage-growth problem step by step with real byte counts before/after VACUUM), or go deeper into deletion vectors' internal mechanics?