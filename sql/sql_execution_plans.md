# SQL Execution Plans and Internals: A Data Engineer's Deep Dive

## Learning Objectives
By the end of this material, you should be able to:
- Read and interpret execution plans across major engines (PostgreSQL, Spark, Snowflake)
- Diagnose performance bottlenecks using plan analysis
- Make informed decisions about query rewrites, indexing, and partitioning
- Understand how the optimizer "thinks" so you can write optimizer-friendly SQL

---

## 1. Foundational Concepts

### 1.1 What Actually Happens When You Run a Query

```
SQL Text → Parser → Logical Plan → Optimizer → Physical Plan → Execution → Results
```

**Parsing**: SQL is checked for syntax validity and converted into an Abstract Syntax Tree (AST).

**Logical Plan (Binding)**: The AST is validated against the catalog (do tables/columns exist? what are their types?) and converted into a tree of relational algebra operations (Scan, Filter, Join, Aggregate, Project). This plan says *what* to do, not *how*.

**Optimization**: The optimizer transforms the logical plan into one or more candidate physical plans, then picks the "best" one based on cost estimates.

**Physical Plan**: This says *how* to execute — which join algorithm, which access method (index vs. full scan), what order to do things in, whether to parallelize.

**Execution**: The engine walks the physical plan tree, typically using a **pull-based iterator model** (each operator calls `next()` on its children) — this is the classic Volcano/Iterator model used by PostgreSQL, MySQL, and most engines.

> **Key mental model**: The optimizer is not "smart" in the human sense — it's a search algorithm exploring a space of equivalent plans using statistics and cost heuristics. When it picks a bad plan, it's usually because your **statistics are stale, your predicates are unsargable, or your data distribution violates its assumptions.**

### 1.2 Rule-Based vs. Cost-Based Optimization

| Type | How it works | Example |
|---|---|---|
| **Rule-Based Optimizer (RBO)** | Applies fixed transformation rules regardless of data | Push filter before join, always use index if one exists |
| **Cost-Based Optimizer (CBO)** | Estimates cost (I/O, CPU, memory) of multiple plans using statistics, picks cheapest | Decide seq scan vs. index scan based on selectivity |

Modern engines (PostgreSQL, Oracle, SQL Server, Spark's Catalyst) use **both**: rules for algebraic simplification (always valid, no cost needed) + cost-based search for physical strategy selection (join order, join algorithm, access path).

**Real-life implication**: If your table statistics are stale (no `ANALYZE`/`VACUUM ANALYZE` run after a bulk load), the CBO makes decisions based on fictional data distributions. This is the #1 cause of "the query was fast yesterday, slow today" incidents after large loads.

---

## 2. Core Physical Operators You Must Recognize

### 2.1 Access Methods (How rows are fetched)

- **Sequential/Full Scan**: Reads entire table. Fine for small tables or when selectivity is low (>~15-20% of rows match).
- **Index Scan**: Uses an index to find matching rows, then fetches from the table (heap/data pages). Efficient for high selectivity.
- **Index-Only Scan**: All needed columns exist in the index — no need to touch the table at all. This is a huge win; it's why **covering indexes** matter.
- **Bitmap Index Scan** (PostgreSQL specific but conceptually universal): Builds a bitmap of matching row locations from index(es), then fetches rows in physical order — reduces random I/O compared to plain index scan when many rows match.

### 2.2 Join Algorithms

| Algorithm | How it works | Best when |
|---|---|---|
| **Nested Loop Join** | For each row in outer table, scan inner table (ideally via index) | Small outer table, indexed inner table, low row counts |
| **Hash Join** | Build hash table on smaller (build) side, probe with larger (probe) side | Large tables, equality joins, no useful index |
| **Sort-Merge Join** | Sort both sides on join key, merge in order | Both inputs already sorted, or output needs sorted order; range joins |

**Real-life diagnostic pattern**: Seeing a **Nested Loop Join over millions of rows** is almost always a red flag — it means the optimizer misjudged cardinality (thought one side would be tiny) or there's a missing index forcing loop-based fallback. This is one of the most common causes of runaway query times in production.

### 2.3 Aggregation Strategies

- **Hash Aggregate**: Builds a hash table keyed by GROUP BY columns. Fast, but memory-bound — spills to disk if the hash table doesn't fit.
- **Sort-based (Stream) Aggregate**: Requires sorted input on GROUP BY keys, then aggregates in a single pass. Used when input is already sorted (e.g., from an index or a prior sort-merge join).

### 2.4 Data Movement Operators (Distributed Engines: Spark, Snowflake, Redshift, BigQuery)

This is where **distributed SQL internals diverge significantly from single-node databases** — critical for data engineers.

- **Shuffle / Exchange**: Redistributes data across nodes/partitions, typically for joins or aggregations on keys not already co-located. **This is usually the most expensive operation in a distributed plan** — it involves network I/O and disk spill.
- **Broadcast**: Instead of shuffling both sides of a join, send the *entire small table* to every node, avoiding a shuffle of the large table. Spark does this automatically for tables under `spark.sql.autoBroadcastJoinThreshold` (default 10MB).
- **Repartition/Coalesce**: Explicit or implicit changes to data distribution across partitions.

---

## 3. Reading Execution Plans: Engine by Engine

### 3.1 PostgreSQL — `EXPLAIN` and `EXPLAIN ANALYZE`

```sql
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT c.name, SUM(o.amount)
FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE o.order_date >= '2024-01-01'
GROUP BY c.name;
```

Sample output:
```
HashAggregate  (cost=15420.32..15430.10 rows=978 width=40)
              (actual time=245.123..245.980 rows=950 loops=1)
  Group Key: c.name
  ->  Hash Join  (cost=8500.00..14200.55 rows=48900 width=36)
                 (actual time=89.234..210.456 rows=47200 loops=1)
        Hash Cond: (o.customer_id = c.id)
        ->  Seq Scan on orders o  (cost=0.00..9800.00 rows=48900 width=12)
                                  (actual time=0.021..95.332 rows=47200 loops=1)
              Filter: (order_date >= '2024-01-01')
              Rows Removed by Filter: 152800
        ->  Hash  (cost=2500.00..2500.00 rows=100000 width=28)
                   (actual time=88.100..88.101 rows=100000 loops=1)
              ->  Seq Scan on customers c (cost=0.00..2500.00 rows=100000 width=28)
```

**How to read it (bottom-up, inside-out):**
1. Innermost/deepest indented operations execute first.
2. **`cost=X..Y`**: X = estimated startup cost, Y = total cost (arbitrary units, not seconds — comparative only).
3. **`rows=N`**: Estimated rows this operator will produce.
4. **`actual time=X..Y`**: Only with `ANALYZE` — real time in ms (startup..total). **This means the query actually ran** — use `ANALYZE` cautiously on production writes/expensive queries; wrap in a transaction and ROLLBACK if testing DML.
5. **`loops=N`**: How many times this node executed (relevant in nested loops — cost is *per loop*, multiply for total).

**The #1 red flag to hunt for**: large discrepancy between **estimated `rows`** and **actual `rows`**. This signals stale/bad statistics.

```
Seq Scan on orders  (cost=0.00..9800.00 rows=48900 width=12)
                    (actual rows=980000 loops=1)   ← 20x underestimate!
```
This kind of misestimate cascades — it might cause the optimizer to choose a Nested Loop expecting a small table, then blow up in production.

**Key BUFFERS metrics** (add `BUFFERS` to EXPLAIN ANALYZE):
- `shared hit`: pages found in cache (good)
- `shared read`: pages read from disk (expensive — indicates cold cache or poor buffer reuse)

### 3.2 Spark — Catalyst Optimizer and `.explain()`

Spark's pipeline has an extra layer specific to distributed/lazy execution:

```
SQL/DataFrame API 
  → Unresolved Logical Plan 
  → Analyzed Logical Plan (resolved against catalog)
  → Optimized Logical Plan (Catalyst rules: predicate pushdown, constant folding, etc.)
  → Physical Plan(s) (Spark Strategies)
  → Selected Physical Plan (cost-based, limited compared to traditional RDBMS)
  → Whole-Stage Code Generation (compiles to JVM bytecode)
  → RDD execution (DAG of stages/tasks)
```

```python
df.explain(mode="formatted")  # or "extended", "cost", "codegen"
```

Sample simplified output:
```
== Physical Plan ==
* HashAggregate (6)
+- Exchange hashpartitioning(customer_id, 200)      (5)
   +- * HashAggregate (4)
      +- * Project (3)
         +- * BroadcastHashJoin [customer_id], [id], Inner, BuildRight  (2)
            :- * Filter (order_date >= '2024-01-01')
            :  +- * FileScan parquet orders  (1)
            +- BroadcastExchange
               +- FileScan parquet customers
```

**Reading it:**
- `*` before an operator = **Whole-Stage Codegen** applied (multiple operators fused into a single compiled function — very efficient, avoids virtual function call overhead per row)
- `Exchange` = **shuffle boundary** = **new Spark stage**. Every Exchange is a potential bottleneck — count them.
- `BroadcastHashJoin` with `BuildRight` = the right table (customers, presumably small) was broadcast to all executors, avoiding a shuffle of `orders`.
- Numbers `(1)`, `(2)` etc. in `formatted` mode map to detailed sections below the tree showing exact statistics, output schema, and partitioning.

**Real-life diagnostic use case**: 
A job runs fine at 10GB but times out at 500GB. `.explain()` shows a **SortMergeJoin with Exchange on both sides** instead of a broadcast join — because the "small" table grew past `autoBroadcastJoinThreshold`. Fix: either increase the threshold (careful with driver memory) or explicitly hint `broadcast(df)` if you know it's still small enough logically but stats say otherwise (e.g., due to file format stats being stale after append-only writes).

**Adaptive Query Execution (AQE)** — Spark 3.x+: Re-optimizes the physical plan *during* execution based on actual runtime statistics (e.g., converts SortMergeJoin to BroadcastJoin after seeing actual shuffle output size, coalesces small shuffle partitions, handles skew by splitting skewed partitions). Look for `AdaptiveSparkPlan` wrapping the plan and `== Final Plan ==` vs `== Current Plan ==` in output — these can differ significantly.

### 3.3 Snowflake — Query Profile

Snowflake gives a **visual DAG** (not text) via Query Profile in the UI, but the concepts map directly:

- **Table Scan**: shows **partitions scanned vs. total partitions** — this is your pruning efficiency metric. If a query scans 100% of partitions when it should scan 5%, your clustering key or filter isn't enabling **micro-partition pruning**.
- **Percentage breakdown per node**: shows relative time spent — quickly points to the bottleneck node.
- **Bytes spilled to local/remote storage**: Critical metric — if present, your `WAREHOUSE` size is too small for the working set (not enough memory), forcing spill to local SSD or (worse) remote S3-backed storage. This is a huge, often invisible cost driver.
- **Join node**: shows input/output row counts — same "estimate vs actual" checking philosophy applies, though Snowflake's optimizer works differently (recompiles based on actual pruned partition stats since it's largely metadata-driven, less classic CBO with stale-stats risk than Postgres).

**Real-life diagnostic use case**: 
Monthly cost review shows warehouse costs spiking. Query Profile reveals **"Bytes spilled to remote storage"** on a large GROUP BY. Root cause: warehouse is `SMALL` but the shuffle/aggregation working set exceeds available memory across nodes. Fix: either resize the warehouse temporarily for that job, or restructure to reduce cardinality before the aggregation (pre-aggregate, filter earlier).

---

## 4. Statistics: The Optimizer's Eyes

Every cost-based decision depends on **cardinality estimation** — predicting how many rows survive each operation.

### What statistics engines maintain:
- **Row counts** (table and per-partition)
- **NDV** (Number of Distinct Values) per column — drives selectivity estimates for equality predicates (`selectivity ≈ 1/NDV`)
- **Histograms** — distribution of values, crucial for range predicates and skewed data
- **Null fraction**
- **Correlation** (PostgreSQL-specific: physical vs. logical ordering — affects index scan cost estimates)

### How estimation compounds and fails:
For independent predicates: `P(A AND B) = P(A) * P(B)` — this is the optimizer's default assumption. 

**Real-life trap**: `WHERE country = 'US' AND state = 'California'` — these are **correlated**, not independent (California implies US). The optimizer multiplies selectivities as if independent, often producing a wildly wrong (usually too low) row estimate, which can then trigger a bad join algorithm choice. PostgreSQL 10+ has **extended statistics** (`CREATE STATISTICS`) to address exactly this: you tell the optimizer to track dependency/correlation between specific columns.

```sql
CREATE STATISTICS stats_country_state (dependencies) ON country, state FROM addresses;
ANALYZE addresses;
```

### Maintenance in practice:
| Engine | Command | When it matters most |
|---|---|---|
| PostgreSQL | `ANALYZE`, `VACUUM ANALYZE` | After bulk load, before running heavy queries |
| Snowflake | Automatic (metadata-driven) | Still watch clustering depth for pruning |
| Spark | `ANALYZE TABLE ... COMPUTE STATISTICS` | Before joins on Hive/Delta tables, esp. for CBO-driven broadcast decisions |
| Redshift | `ANALYZE`, plus `VACUUM` for reclaiming space | After large COPY/DELETE operations |

**Real-life incident pattern**: Nightly ETL does a full table truncate + reload of a dimension table. Downstream BI queries suddenly regress from 2s to 2min. Root cause: truncate+load doesn't auto-trigger stats refresh in some engines/configs; the optimizer still thinks the table has old row counts/distribution, chooses a Nested Loop assuming a tiny table, and the query craters. **Fix**: add explicit `ANALYZE`/`COMPUTE STATISTICS` as a step immediately after load in the pipeline DAG (e.g., an Airflow task after the load task).

---

## 5. Index Internals for Data Engineers

You don't need DBA-level index tuning, but you need to understand **why an index helps or doesn't** — this constantly affects pipeline and query design.

### B-Tree (default, most common)
- Sorted structure, supports equality and range predicates (`=`, `<`, `>`, `BETWEEN`), sorting, `ORDER BY` avoidance.
- **Leftmost prefix rule**: composite index `(a, b, c)` supports queries filtering on `a`, `(a,b)`, or `(a,b,c)` — but **not** `b` alone or `c` alone efficiently.

### Why an index might be *ignored* even if it exists:
1. **Low selectivity** — if the predicate matches >15-20% of rows, a full scan is cheaper (avoids random I/O of index+heap lookups).
2. **Function on indexed column**: `WHERE UPPER(email) = 'X'` won't use a plain index on `email` — you need a **functional/expression index**: `CREATE INDEX ON users (UPPER(email))`.
3. **Implicit type casting**: comparing a `text` column to an `integer` literal can silently disable index usage in some engines.
4. **Leading wildcard LIKE**: `LIKE '%smith'` can't use a standard B-tree index (no defined starting point); `LIKE 'smith%'` can.
5. **OR conditions** across different columns without matching composite/bitmap support may force full scans (bitmap scans can help by combining multiple indexes).

### Covering Index Pattern (very high-leverage for ETL/reporting queries)
```sql
-- Query: frequently fetch order status by customer, filtered by date
SELECT customer_id, status FROM orders 
WHERE order_date >= '2024-01-01' AND customer_id = 123;

-- Covering index: satisfies filter AND select columns without touching heap
CREATE INDEX idx_orders_covering ON orders (customer_id, order_date) INCLUDE (status);
```
This produces an **Index-Only Scan** — zero heap fetches, dramatically less I/O. Extremely relevant for **high-frequency API/serving queries backed by a data warehouse extract or operational store**.

---

## 6. Partitioning & Pruning (The Distributed/Warehouse Equivalent of Indexing)

For big data engines, **partition pruning** is often more impactful than any index.

### Concept
If a table is partitioned by `order_date`, and your query filters `WHERE order_date = '2024-06-01'`, the engine should **skip reading partitions/files outside that range entirely** — never even opening them.

### How to verify pruning is happening (this is a *must-check* skill):

**Spark**:
```
== Physical Plan ==
+- FileScan parquet orders[...]
   PartitionFilters: [isnotnull(order_date#12), (order_date#12 = 2024-06-01)]
   PushedFilters: [...]
```
If `PartitionFilters` is empty and your filter appears only in `PushedFilters` (or not at all), **pruning is not happening at the partition level** — you're reading all partitions and filtering post-hoc, or worse, not pushing down at all.

**Common pruning failures (real-life anti-patterns)**:
```sql
-- BAD: function wraps partition column, defeats pruning
WHERE YEAR(order_date) = 2024

-- GOOD: direct comparison on partition column
WHERE order_date >= '2024-01-01' AND order_date < '2025-01-01'
```
```sql
-- BAD: partition column compared to derived/casted type mismatch
WHERE CAST(order_date AS STRING) = '2024-06-01'
```

**Snowflake specific**: Micro-partition pruning depends on the **clustering key** (or natural load order if unclustered). If data is loaded out of order relative to your common filter columns, pruning degrades over time — check `SYSTEM$CLUSTERING_INFORMATION()` and consider `ALTER TABLE ... CLUSTER BY` for large, frequently-filtered fact tables.

---

## 7. Join Order and the Combinatorial Explosion Problem

For a query joining N tables, there are `N!` possible join orders (more with different tree shapes). Optimizers can't exhaustively search this for large N.

- **PostgreSQL**: Exhaustive search up to `join_collapse_limit` (default 8 tables), then switches to **Genetic Query Optimizer (GEQO)** — a heuristic/randomized search. This is why **queries with 10+ joins can have unstable, hard-to-predict plans** across runs.
- **Spark/distributed engines**: Rely heavily on broadcast vs. shuffle join decisions and often need **hints** for complex multi-way joins since statistics for intermediate join results are much harder to estimate accurately in a distributed setting (this compounds fast — cardinality estimation error grows multiplicatively with join depth).

**Real-life practice**: For complex analytical queries with many joins (common in star-schema BI queries with 8+ dimension tables), consider:
1. Materializing intermediate results (CTEs materialized as temp tables, or checkpoint in Spark) to give the optimizer accurate stats to work from at each stage, rather than one giant multi-join query where estimation error compounds.
2. Explicit join hints (`/*+ BROADCAST(dim_table) */` in Spark SQL, `pg_hint_plan` extension in Postgres) when you know better than the optimizer — but treat this as a last resort, document why, and revisit periodically as data grows.

---

## 8. Practical Diagnostic Workflow (Bringing It Together)

When a query is slow, work through this checklist:

```
1. Get the actual plan (EXPLAIN ANALYZE / .explain() / Query Profile)
   ↓
2. Find the most expensive/time-consuming node
   (biggest actual time delta, most bytes spilled, largest exchange)
   ↓
3. Check estimated vs actual rows at that node
   ├─ Big mismatch? → Stats problem (ANALYZE / COMPUTE STATISTICS)
   └─ Accurate estimate but still slow? → Continue
   ↓
4. Check access method at scan nodes
   ├─ Seq/Full scan on large table with selective filter? → Missing index or partition pruning failure
   └─ Check partition/bitmap pruning stats
   ↓
5. Check join algorithm
   ├─ Nested Loop on large row counts? → Cardinality misestimate or missing index
   └─ Shuffle/Exchange present? → Is it necessary? Can broadcast or repartition earlier help?
   ↓
6. Check memory/spill indicators
   (Snowflake: bytes spilled; Spark: disk spill in stage UI; Postgres: temp file usage in logs)
   ↓
7. Rewrite / re-index / re-partition / adjust resources — then re-measure
```

**Golden rule**: Never change more than one thing at a time when tuning — you need to isolate which change caused which effect, since these systems are non-linear (fixing one bottleneck often reveals or shifts to the next one).

---

## 9. Quick Reference: Cross-Engine Terminology Map

| Concept | PostgreSQL | Spark | Snowflake |
|---|---|---|---|
| Plan inspection | `EXPLAIN ANALYZE` | `.explain()` | Query Profile UI |
| Data redistribution | (single node, N/A) | `Exchange` | (automatic, hidden) |
| Small-table optimization | — | Broadcast Join | — (auto via micro-partition metadata) |
| Partition skipping | Constraint exclusion / partition pruning | `PartitionFilters` | Micro-partition pruning |
| Stats refresh | `ANALYZE` | `COMPUTE STATISTICS` | Automatic |
| Memory overflow signal | Temp file spill in logs | Disk spill in Stage UI | "Bytes spilled to storage" |
| Runtime re-optimization | None (static plan) | Adaptive Query Execution | Automatic recompilation |

---

## Suggested Hands-On Exercises

1. Take a query you run regularly in production. Run `EXPLAIN ANALYZE` and find one node where estimated rows differ from actual by >2x. Trace why.
2. In Spark, take a join you know involves a small dimension table. Force a `SortMergeJoin` by disabling broadcast (`spark.sql.autoBroadcastJoinThreshold=-1`) and compare `.explain()` + runtime against the broadcast version.
3. In Snowflake, find a query scanning >50% of partitions on a large table. Investigate whether a clustering key change would help, using `SYSTEM$CLUSTERING_INFORMATION`.
4. Create a covering index for one of your top 5 slowest reporting queries and confirm via plan that it becomes an Index-Only Scan.