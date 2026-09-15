# Adaptive Query Execution (AQE) in Spark: A Comprehensive Study Guide

## 1. Why AQE Exists: The Core Problem

Before diving into mechanics, understand the fundamental issue AQE solves:

**Spark's Catalyst Optimizer works with statistics gathered *before* query execution** (table sizes, column cardinality estimates, etc.). These estimates are often wrong because:

- Statistics become stale as data changes
- Complex transformations (UDFs, filters, joins) make cardinality estimation nearly impossible to predict accurately
- Data skew is invisible until you actually read the data

**The consequence:** Spark builds an execution plan based on guesses. If those guesses are wrong, you get:
- Wrong join strategies (broadcast vs. shuffle)
- Poor partition sizing (too many tiny tasks or too few huge ones)
- Skewed partitions that create straggler tasks

AQE's core idea: **re-optimize the query plan during execution, using real runtime statistics instead of pre-execution estimates.**

---

## 2. Where AQE Fits in Query Execution

To understand AQE, you need to understand Spark's execution model:

```
Logical Plan → Optimized Logical Plan → Physical Plan → Execution
```

Spark executes queries in **stages**, separated by **shuffle boundaries** (exchanges). AQE exploits this natural checkpoint:

> After each shuffle stage completes, Spark *knows the actual size and distribution* of the shuffled data — no more guessing.

> During a shuffle, each task writes its output partitions to disk and, as a byproduct, counts exactly how many bytes it wrote to each destination partition. This byte count (called a MapStatus) is sent back to the driver, which aggregates all the MapStatus reports into a central registry called the MapOutputTracker.

AQE re-evaluates the plan **at each stage boundary**, using this real data to decide how to execute the next stage.

```
Stage 1 (scan+filter) → SHUFFLE → [AQE re-optimizes here] → Stage 2 (join) → SHUFFLE → [AQE re-optimizes] → Stage 3
```

This is the single most important mental model: **AQE = re-planning at materialization points, not a one-time optimization.**

---

## 3. The Three Core Features of AQE

AQE has exactly three main optimizations. Master each one deeply — don't just memorize names.

### 3.1 Dynamically Coalescing Shuffle Partitions

**Problem it solves:** You set `spark.sql.shuffle.partitions=200` at the start of your job. But after a filter reduces your data by 95%, you don't need 200 partitions — you need 10. Result without AQE: 190 tasks processing near-empty partitions, wasting scheduling overhead.

**How it works:**
1. Spark writes shuffle output into many small partitions.
2. AQE checks the actual size of each partition post-shuffle.
3. Adjacent small partitions are **merged (coalesced)** into fewer, right-sized partitions before the next stage reads them.

**Key config:**
```
spark.sql.adaptive.coalescePartitions.enabled = true (default)
spark.sql.adaptive.coalescePartitions.minPartitionSize
spark.sql.adaptive.advisoryPartitionSizeInBytes = 64MB (default target size)
```

**Real-life use case:**
You have a pipeline reading 10 years of transaction logs, but a query filters to `WHERE year = 2024`. Without AQE, if `shuffle.partitions=200`, you'd get 200 tiny output files/tasks for what's really 1/10th of the data. AQE coalesces this down to ~20 partitions automatically — no manual `repartition()` needed, and no more tuning `shuffle.partitions` per query.

---

### 3.2 Dynamically Switching Join Strategies

**Problem it solves:** Spark decides whether to use **Broadcast Hash Join** (fast, no shuffle) or **Sort-Merge Join** (slower, shuffles both sides) based on *pre-execution* size estimates. If a table is estimated as "large" but turns out small after filtering, you're stuck with an expensive sort-merge join.

**How it works:**
1. Query starts with a Sort-Merge Join plan (the "safe" default for large tables).
2. After the shuffle/filter stage, AQE checks actual materialized size of each join side.
3. If one side is now below the broadcast threshold, **AQE converts the join to Broadcast Hash Join on the fly.**

**Key config:**
```
spark.sql.adaptive.autoBroadcastJoinThreshold  
(falls back to spark.sql.autoBroadcastJoinThreshold if unset, default 10MB)
```

**Real-life use case:**
```sql
SELECT * FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE c.region = 'APAC'  -- filters customers from 50M rows to 200K rows
```
Static optimizer sees `customers` as 50M rows → plans Sort-Merge Join. After the filter executes, AQE sees the *actual* post-filter size (200K rows, small enough to broadcast) → switches to Broadcast Hash Join, eliminating an expensive shuffle on the `orders` side entirely.

**Interview-relevant detail:** This only works for joins where AQE can re-plan *after* seeing intermediate results — it applies at the point where the query plan has a materialized shuffle stage to inspect.

---

### 3.3 Dynamically Optimizing Skew Joins

**Problem it solves:** Data skew — a few partition keys have disproportionately more data (e.g., `customer_id = NULL` or a "popular product" in an e-commerce fact table). In a Sort-Merge Join, one task handles the skewed partition while all others finish in seconds — that one task becomes a **straggler**, dominating total job time.

**How it works:**
1. After shuffle, AQE examines partition sizes on both sides of the join.
2. If a partition exceeds a threshold (relative to median partition size), it's flagged as **skewed**.
3. AQE **splits the skewed partition into smaller sub-partitions**.
4. The corresponding partition on the *other* side of the join is **duplicated** across these splits so the join semantics remain correct.
5. These sub-partition joins run in parallel instead of one giant task.

**Key config:**
```
spark.sql.adaptive.skewJoin.enabled = true (default)
spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5 (default: 5x median size = skewed)
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 256MB (default)
```

**Real-life use case:**
An e-commerce platform joins `events` (clickstream) with `products` on `product_id`. One flash-sale product accounts for 40% of all clicks that day. Without AQE, the reducer handling that product_id's partition runs for 45 minutes while 199 other reducers finish in 30 seconds — total job time = 45 minutes. AQE detects this partition is >5x the median size, splits it into 10 sub-partitions, and processes them in parallel → job finishes in ~5 minutes.

**Important nuance:** Skew handling only works for **Sort-Merge Joins**, not broadcast joins (broadcast joins don't shuffle, so there's no skewed partition to split).

---

## 4. Enabling AQE

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")  # default: true since Spark 3.2
```

All three sub-features have their own enable flags, but they only activate when the parent flag is `true`.

---

## 5. Critical Prerequisite: AQE Needs Shuffle Boundaries

**This is the most commonly misunderstood aspect of AQE.**

AQE **cannot optimize a query with no shuffle**. If your query is a simple filter+scan with no join, aggregation, or explicit repartition, there's no stage boundary for AQE to intervene at — nothing to re-optimize.

```python
# No shuffle — AQE has nothing to adapt
df.filter(df.year == 2024).select("id", "amount")

# Has shuffle (join) — AQE can kick in after the shuffle stage
df1.join(df2, "id")
```

This matters when debugging: if you expect AQE to fix a problem but see no change in the physical plan, check whether a shuffle boundary actually exists.

---

## 6. How to Verify AQE Is Working (Practical Skill)

Don't just trust configs — verify behavior:

```python
df.explain("formatted")  # or explain(True)
```

Look for:
- `AdaptiveSparkPlan isFinalPlan=true` — confirms AQE ran and this is the final, re-optimized plan
- `CustomShuffleReader` / `AQEShuffleRead` — indicates coalesced or skew-split partitions
- Compare `BroadcastHashJoin` vs `SortMergeJoin` in the final plan vs. the initial plan

**Spark UI approach:**
- SQL tab → click on query → compare the plan graph before/after execution
- Look at task duration distribution in the Stages tab — skew join optimization should show more uniform task durations post-AQE vs. pre-AQE

---

## 7. AQE vs. Static Optimization Techniques (Comparison Table)

| Technique | When Applied | Based On | Manual Effort |
|---|---|---|---|
| Static broadcast hint (`broadcast()`) | Compile time | Developer knowledge | High — must know data size in advance |
| `spark.sql.shuffle.partitions` tuning | Compile time / config | Global static value | Medium — one-size-fits-all per job |
| Manual `repartition()`/`coalesce()` | Compile time | Developer's estimate | High |
| **AQE (all 3 features)** | **Runtime, per-stage** | **Actual materialized data** | **Low — automatic** |

**Key insight for interviews:** AQE doesn't replace the need to understand these concepts — it reduces the need for *manual, static* tuning of them. You still need to know *why* skew or partition sizing matters to interpret AQE's behavior and diagnose when it's *not* helping.

---

## 8. Limitations and Gotchas (Where Real Expertise Shows)

1. **AQE only re-plans at shuffle boundaries** — it can't fix a bad join strategy mid-stage; it waits for the stage to complete first.

2. **Doesn't help with skew *within* a single partition read from source** (e.g., skewed Parquet file sizes before any shuffle) — that's a data layout problem, not something AQE addresses.

3. **Broadcast join conversion has a ceiling** — if AQE decides to broadcast a table that turns out to be much larger than expected (rare, but possible with very unusual filters), you can get driver OOM. AQE reduces risk here but doesn't eliminate the need for sane broadcast thresholds.

4. **Doesn't optimize across independent queries** — AQE is scoped to a single query's execution; it won't hold data in memory smartly across multiple jobs.

5. **Interacts with caching carefully** — if you `.cache()` a DataFrame, the cached plan is fixed; AQE's benefits are diminished for cached data reused across contexts since the "stage" boundary logic differs.

6. **Local mode / very small clusters** — benefits are less visible since stragglers/skew have less relative impact when everything is small.

7. **Skew factor tuning matters** — `skewedPartitionFactor=5` might be too conservative or too aggressive depending on your data profile; blindly trusting defaults can hide real skew issues in edge cases (e.g., moderate skew that's still 3x, not flagged).

---

## 9. Practical Debugging Workflow (Applying This Knowledge)

When a Spark job is slow, use AQE-awareness as a diagnostic lens:

1. **Check the Spark UI Stages tab** — is one task taking 10x longer than others? → Likely skew. Confirm AQE skew join is enabled; if it is and skew persists, your `skewedPartitionThresholdInBytes` may need lowering, or the skew exists on a *non-shuffle* read (needs manual salting).

2. **Check for excessive small tasks** — hundreds of tasks finishing in milliseconds? → Check if `coalescePartitions` is enabled; if it is and you still see this, verify a shuffle boundary actually exists in your plan.

3. **Check join type in final plan** — `explain()` shows `SortMergeJoin` where you expected `BroadcastHashJoin`? → Check actual post-filter data size; it may genuinely exceed the broadcast threshold, or AQE might not have had a shuffle stage to inspect before the join (e.g., broadcast decision made before any AQE re-planning opportunity existed).

4. **When AQE isn't enough:** Manual **salting** for extreme skew (single key with billions of rows) — AQE splits skewed partitions but if one key alone is too large even after splitting, you need to manually add a random salt to the join key on both sides to force further distribution.

---

## 10. Quick Reference: Key Configs Summary

```python
# Master switch
spark.sql.adaptive.enabled = true

# Partition coalescing
spark.sql.adaptive.coalescePartitions.enabled = true
spark.sql.adaptive.advisoryPartitionSizeInBytes = 64MB

# Join strategy switching
spark.sql.adaptive.autoBroadcastJoinThreshold = 10MB (inherits from non-adaptive setting if unset)

# Skew join handling
spark.sql.adaptive.skewJoin.enabled = true
spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 256MB
```

---

## Suggested Next Steps for Mastery

To solidify this into working knowledge rather than memorized facts, I'd suggest we go deeper on either:
- **Data skew and salting techniques** (natural extension — AQE handles moderate skew, salting handles extreme skew)
- **Spark's Catalyst Optimizer internals** (understand *why* pre-execution estimates are wrong in the first place)
- **Shuffle mechanics** (understand what's actually happening at those boundaries AQE exploits)

Which direction would help most given what you're working on?