# Bucketing in Spark: Complete Study Guide

## 1. Conceptual Foundation

### What is Bucketing?

Bucketing is a data organization technique that pre-partitions data into a **fixed number of files** based on the **hash value of one or more columns**. Unlike partitioning (which creates directories), bucketing distributes data into a predetermined number of buckets (files) within each partition or table.

```
hash(bucket_column) % num_buckets = bucket_number
```

### Bucketing vs Partitioning — The Critical Distinction

| Aspect | Partitioning | Bucketing |
|---|---|---|
| Storage structure | Creates directories | Creates fixed number of files |
| Cardinality fit | Low cardinality columns | High cardinality columns |
| Number of divisions | Dynamic (based on distinct values) | Fixed (defined upfront) |
| Skew risk | High for skewed keys | Controlled — you set bucket count |
| Use case | Filtering (predicate pushdown) | Joins, aggregations, sampling |
| Small file risk | High with high cardinality | Controlled by bucket count |

**Key mental model**: Partitioning answers "*where does this data live?*" Bucketing answers "*how is data distributed within a location for efficient joins/aggregations?*"

### Why Bucketing Exists — The Core Problem It Solves

Spark joins/aggregations on large datasets typically require a **shuffle** — reading data across the cluster, hashing keys, and redistributing rows so matching keys land on the same executor. This is expensive: network I/O, serialization, disk spill.

**Bucketing pre-shuffles the data at write time**, so at read time (join/aggregation), Spark can skip the shuffle entirely if bucketing metadata matches — because rows with the same key are *already* co-located in the same bucket file.

---

## 2. How Bucketing Works Internally

### Write Path

```python
df.write \
  .bucketBy(8, "user_id") \
  .sortBy("user_id") \
  .saveAsTable("bucketed_users")
```

Under the hood:
1. Spark computes `hash(user_id) % 8` for every row
2. Rows are grouped by bucket number
3. Each bucket is written as one or more files, named like:
   ```
   part-00000-xxxx_00000.c000.snappy.parquet  (bucket 0)
   part-00000-xxxx_00001.c000.snappy.parquet  (bucket 1)
   ```
4. Metadata (bucket count, bucketing columns, sort columns) is stored in the **Hive metastore** (or catalog) — this is crucial, because Spark needs this metadata at read time to know it *can* skip the shuffle.

> **Important**: Bucketing requires `saveAsTable()` — it does **not** work with `df.write.save()` to plain file paths, because bucketing metadata must live in a catalog/metastore. This is a common gotcha.

### Read Path — Bucket Pruning & Join Optimization

**Case 1: Bucket Pruning (Filter)**
```python
spark.sql("SELECT * FROM bucketed_users WHERE user_id = 12345")
```
Spark computes `hash(12345) % 8` and reads **only that one bucket file**, skipping the rest — similar to partition pruning but for equality filters on bucketed columns.

**Case 2: Sort-Merge Join without Shuffle**

This is the primary use case. If two tables are:
- Bucketed on the join key
- Bucketed into the **same number of buckets**

Spark can perform a **Sort-Merge Join (SMJ)** by directly matching bucket N of table A with bucket N of table B — **no shuffle exchange needed**.

```
Normal SMJ:  Scan A → Shuffle A → Sort → 
             Scan B → Shuffle B → Sort → Merge Join

Bucketed SMJ: Scan A (pre-sorted bucket) → 
              Scan B (pre-sorted bucket) → Merge Join
```

You can verify this in the physical plan — look for the **absence of `Exchange`** operator:

```python
df.explain()
# Look for: no "Exchange hashpartitioning" before the SortMergeJoin
```

---

## 3. Requirements for Shuffle-Free Bucketed Joins

This is where most real-world bucketing implementations fail silently — Spark falls back to a normal shuffle join without warning. Requirements:

1. **Same number of buckets** in both tables (not just same bucketing column)
2. **Join key = bucketing key** exactly
3. Both tables registered with bucketing metadata (via `saveAsTable`)
4. `spark.sql.sources.bucketing.enabled` = `true` (default since Spark 2.x)
5. No intervening transformation that changes row distribution (e.g., a filter is fine, but a `repartition()` before the join breaks bucket-awareness)
6. Ideally, **sortBy** the same column too — enables merge join without an in-memory sort step

**Case where it silently fails**: bucketing on `user_id` with 8 buckets vs. 16 buckets — Spark **cannot** align buckets 1:1, so it shuffles anyway.

---

## 4. Choosing the Number of Buckets

No universal formula, but practical heuristics:

- **Target file size**: aim for 100MB–1GB per bucket file (Parquet compressed). 
  ```
  num_buckets ≈ total_table_size / target_file_size
  ```
- **Power-of-2 is not required** but is common convention for even hash distribution mentally — not a technical constraint.
- **Avoid too many small buckets**: creates small-file problem, overhead in file listing/metadata, poor I/O throughput (especially on HDFS/S3 — S3 has per-request latency, so many tiny files kill performance).
- **Avoid too few large buckets**: reduces parallelism, each task processes more data, can cause skew if the hash distribution isn't uniform on that column.
- **Consider cluster parallelism**: number of buckets often aligned to a multiple of total executor cores for even task distribution.

### Data Skew Consideration

If the bucketing column has skewed value distribution (e.g., 90% of orders belong to 10 customers), hash-based bucketing will still create **uneven bucket sizes** — some buckets get more rows than others. Bucketing does not fix skew; it only fixes shuffle avoidance for joins. Combine with **salting** techniques if skew is severe.

---

## 5. Real-World Use Cases

### Use Case 1: Recurring Large Fact-Dimension Joins

**Scenario**: A retail data warehouse joins a 500GB `orders` fact table with a 200GB `customers` table on `customer_id` — daily, in multiple downstream ETL jobs.

**Without bucketing**: every job pays the full shuffle cost — reading, hashing, and redistributing terabytes of data across the network, repeatedly, every single run.

**With bucketing**: 
```python
orders_df.write.bucketBy(50, "customer_id").sortBy("customer_id").saveAsTable("orders_bucketed")
customers_df.write.bucketBy(50, "customer_id").sortBy("customer_id").saveAsTable("customers_bucketed")
```
The shuffle cost is paid **once at write time**. Every subsequent join reads pre-bucketed data — dramatically reducing daily ETL runtime and cluster cost.

**Trade-off to mention**: this only pays off if the tables are joined *repeatedly*. For one-off joins, bucketing overhead (write-time shuffle + sort) isn't worth it.

### Use Case 2: Point Lookups on a High-Cardinality Key

**Scenario**: A service layer queries a 1TB event log table by `session_id` (millions of unique values — bad partitioning candidate due to too many directories).

**Solution**: Bucket by `session_id` into, say, 200 buckets. Point queries (`WHERE session_id = 'xyz'`) trigger bucket pruning — Spark scans one bucket file instead of the whole table.

**Why not partition instead**: partitioning by `session_id` would create millions of tiny directories — metadata explosion, terrible for the file system/metastore. Bucketing handles high cardinality gracefully because bucket count is fixed regardless of key cardinality.

### Use Case 3: Combining Partitioning + Bucketing

**Scenario**: A time-series IoT sensor table, queried by date range **and** joined on `device_id` for enrichment.

```python
df.write \
  .partitionBy("event_date") \
  .bucketBy(20, "device_id") \
  .sortBy("device_id") \
  .saveAsTable("sensor_readings")
```

This gives:
- **Partition pruning** on `event_date` (directory-level skip)
- **Bucket pruning/join optimization** on `device_id` within each date partition

This layered strategy is extremely common in production lakehouse tables — partition by ingestion/business date, bucket by join/lookup key.

### Use Case 4: Reducing Shuffle in Aggregations

**Scenario**: Frequent `GROUP BY user_id` aggregations on a large clickstream table.

Since rows with the same `user_id` are already co-located in the same bucket, Spark's aggregation can perform a partial local aggregation per bucket without a full shuffle exchange for the grouping stage — same principle as joins, applied to `groupBy`/aggregate operations.

---

## 6. Common Pitfalls (Real-World Failure Modes)

| Pitfall | Consequence | Fix |
|---|---|---|
| Using `df.write.save(path)` instead of `saveAsTable` | Bucketing metadata lost, no optimization | Always use `saveAsTable` for bucketed tables |
| Mismatched bucket counts across joined tables | Silent fallback to full shuffle join | Standardize bucket count across related tables |
| Repartitioning/coalescing after reading bucketed table, before join | Breaks bucket co-location, forces shuffle | Avoid unnecessary repartition between read and join |
| Too many buckets on a small table | Small-file problem, S3 listing overhead | Size buckets to ~100MB–1GB target |
| Assuming bucketing fixes data skew | Uneven bucket sizes, straggler tasks | Combine with salting for genuinely skewed keys |
| Forgetting `sortBy` | Sort-merge join still needs an extra in-memory sort per bucket | Add `sortBy` matching the join/bucket key when join-heavy |
| Bucketing a column rarely used in joins/filters | Wasted write-time shuffle cost, no read benefit | Bucket only on genuinely reused join/lookup keys |
| Not verifying via `explain()` | Believing optimization is active when it isn't | Always check physical plan for absence of `Exchange` |

---

## 7. How to Verify Bucketing is Working (Practical Skill)

```python
# 1. Check the physical plan for the join
df1.join(df2, "user_id").explain(True)
# Look for: absence of "Exchange hashpartitioning" between Scan and SortMergeJoin

# 2. Inspect table metadata
spark.sql("DESCRIBE FORMATTED bucketed_users").show(100, False)
# Look for: "Num Buckets", "Bucket Columns", "Sort Columns"

# 3. Check actual files written
%fs ls /path/to/table  # or dbutils.fs.ls
# Count files per partition — should roughly match bucket count
```

---

## 8. Bucketing vs. Modern Alternatives (Context for Decision-Making)

Worth knowing for real-world architecture discussions:

- **Delta Lake / Iceberg / Hudi with Z-Ordering or Liquid Clustering**: many modern lakehouse formats provide alternatives to classic Hive bucketing (e.g., Databricks' **Liquid Clustering**, **Z-Order**) that achieve similar data co-location benefits with more flexibility (no fixed bucket count, adaptive to changing query patterns).
- **AQE (Adaptive Query Execution)**: Spark 3.x's AQE can dynamically optimize skewed joins and coalesce shuffle partitions — reduces (but doesn't eliminate) the need for manual bucketing in some workloads.
- **When bucketing is still the right call**: stable, well-known join patterns on large tables, repeated over many jobs (classic data warehouse fact/dimension model), where the write-time cost is amortized over many reads.

---

## 9. Quick Decision Framework

Ask these questions before bucketing a table:

1. Is this table joined/aggregated on the **same key repeatedly** across many jobs? → Good bucketing candidate
2. Is the key **high cardinality**? → Bucket instead of partition
3. Is the key **low cardinality with time/category semantics**? → Partition instead (or combine both)
4. Will I be using `saveAsTable`, and is my downstream consumer also on the same bucket count? → Confirm before committing
5. Is the write-time cost justified by read-time reuse frequency? → If one-off, skip bucketing