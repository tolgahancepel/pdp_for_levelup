# Window Functions: Aggregate Behavior, Execution Internals & Performance

## Part 1 — The Mental Model

### 1.1 What a window function actually is

A window function computes a value **per row**, using a set of *other rows related to it*, without collapsing the result set. This is the single most important distinction from `GROUP BY`:

| | `GROUP BY` + aggregate | Window function |
|---|---|---|
| Output rows | 1 per group | 1 per input row |
| Row detail preserved | No | Yes |
| Can mix aggregate + row-level columns | No (needs join/subquery) | Yes, natively |

```sql
-- GROUP BY: collapses
SELECT dept, SUM(salary) FROM emp GROUP BY dept;

-- Window: keeps every employee row AND the department total
SELECT dept, name, salary,
       SUM(salary) OVER (PARTITION BY dept) AS dept_total
FROM emp;
```

Mentally, a window function is: **"for this row, look at a defined neighborhood of rows, and reduce that neighborhood to a value."** The neighborhood is the *window frame*.

### 1.2 The four parts of `OVER()`

```sql
func(...) OVER (
    [PARTITION BY partition_cols]
    [ORDER BY order_cols]
    [frame_clause]   -- ROWS|RANGE|GROUPS BETWEEN ... AND ...
)
```

- **PARTITION BY** — splits rows into independent groups. Each partition is processed in isolation (no cross-partition leakage). Conceptually equivalent to a `GROUP BY` key, but rows aren't merged.
- **ORDER BY** — defines a *sequence* inside each partition. Required for ranking functions (`ROW_NUMBER`, `RANK`), `LAG`/`LEAD`, and any frame that isn't "the whole partition."
- **Frame clause** — defines *which rows relative to the current row* participate in the aggregate.
- No `PARTITION BY` = one giant partition = the entire result set is the window. This has serious performance consequences (see Part 3).

---

## Part 2 — Frame Semantics: The Source of Most Bugs

### 2.1 The default frame trap

This is the single most common source of incorrect window-function results:

- If `ORDER BY` is **present** but no frame is specified, the default is:
  ```
  RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ```
- If `ORDER BY` is **absent**, the default is:
  ```
  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  -- whole partition
  ```

People assume "current row" behavior everywhere once `ORDER BY` is added, and it silently changes the *entire partition* into a *running/cumulative* computation.

```sql
-- Innocent looking:
SELECT id, amount,
       SUM(amount) OVER (PARTITION BY cust_id ORDER BY txn_date) AS total
FROM txns;
```
This is **not** "total spend per customer" — it's a **running total up to that date**, because adding `ORDER BY` silently activated the default cumulative frame. To get a true partition-wide total you need `ORDER BY` removed, or an explicit `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.

### 2.2 ROWS vs RANGE vs GROUPS — they are not interchangeable

- **ROWS** — physical offset: "N rows before/after this one," counted positionally.
- **RANGE** — logical offset based on the *value* of the `ORDER BY` column. Rows with the **same order-by value are treated as a single peer group** and are all included together, even in `CURRENT ROW` framing.
- **GROUPS** — like RANGE's peer-group idea, but the offset count is in units of *distinct groups*, not physical rows or value distance.

**Concrete failure case (ties break RANGE):**

```sql
-- data: (cust_id=1, txn_date='2024-01-01', amount=100)
--       (cust_id=1, txn_date='2024-01-01', amount=50)   <- same date!
--       (cust_id=1, txn_date='2024-01-02', amount=30)

SELECT txn_date, amount,
       SUM(amount) OVER (PARTITION BY cust_id ORDER BY txn_date
                          RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM txns;
```
Result: **both** 2024-01-01 rows get `150` (100+50), because `RANGE` treats same-date rows as one peer group and includes the entire group for every member. If you wanted a strict row-by-row running total (first row = 100, second = 150), you need `ROWS` instead of `RANGE`:

```sql
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
```

**Rule of thumb:**
- Use `ROWS` for "the last N rows" / deterministic row-by-row running calculations. Always pair with a tiebreaker in `ORDER BY` (e.g., a surrogate id) to make row order deterministic.
- Use `RANGE` intentionally when you want value-based windows, e.g., "all events within the last 7 days" using an interval: `RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW`.

### 2.3 Logical (not physical) SQL execution order

Understanding *when* window functions execute in the logical pipeline explains several "why can't I do this" moments:

```
FROM / JOIN → WHERE → GROUP BY → HAVING → WINDOW FUNCTIONS → SELECT list
  → DISTINCT → ORDER BY → LIMIT/OFFSET
```

Consequences:
1. **You cannot filter on a window function result in `WHERE`** — it hasn't been computed yet at that stage.
   ```sql
   -- INVALID:
   SELECT id, ROW_NUMBER() OVER (...) rn FROM t WHERE rn = 1;

   -- CORRECT: wrap in CTE/subquery so WHERE runs on already-computed column
   WITH ranked AS (
     SELECT id, ROW_NUMBER() OVER (PARTITION BY key ORDER BY ts DESC) rn FROM t
   )
   SELECT * FROM ranked WHERE rn = 1;
   ```
2. Window functions **can** reference the results of `GROUP BY`/aggregates (they run after), enabling patterns like "rank departments by their total salary."
3. Because window functions run *before* `SELECT`'s final projection and `LIMIT`, the engine still must compute the window over the **full pre-filtered dataset** — there is no way to "compute only for the top 10 rows" without first computing for everyone. This directly impacts performance (see 3.4).

---

## Part 3 — Execution Behavior & Performance Internals

This is the part that actually determines whether your window function query runs in seconds or hangs the cluster.

### 3.1 The physical execution recipe

Almost every engine (Postgres, Spark, Snowflake, BigQuery) executes a window function the same way physically:

1. **Shuffle/redistribute** rows so that all rows of the same `PARTITION BY` key land on the same node/task (skip this step if there's no partition key, or if data is already co-located, e.g., pre-bucketed).
2. **Sort** rows within each partition by the `ORDER BY` columns.
3. **Scan sequentially** through the sorted partition, maintaining frame boundaries and an aggregate accumulator, emitting one output row per input row.
4. If multiple window specs with **different** `PARTITION BY`/`ORDER BY` exist in the same query, repeat steps 1–3 for each distinct spec.

This means a window function query has a **mandatory sort + potential shuffle** cost, independent of how simple the aggregate is. There is no way to compute `ROW_NUMBER() OVER (ORDER BY x)` without sorting by `x`.

### 3.2 How the accumulator scan actually works (why frame type matters for cost)

The naive way to compute a windowed aggregate is: for every row, re-scan its whole frame and recompute — **O(n·w)** where `w` is average frame size. Real engines avoid this using **incremental (sliding) aggregation**, but only in some cases:

| Frame shape | Technique | Complexity |
|---|---|---|
| `UNBOUNDED PRECEDING → CURRENT ROW` (running total/rank) | Single forward pass, accumulate, never remove | O(n) |
| `UNBOUNDED PRECEDING → UNBOUNDED FOLLOWING` (whole partition) | One pass to compute the aggregate once, broadcast to all rows | O(n) |
| Bounded sliding frame, e.g. `3 PRECEDING AND 3 FOLLOWING`, aggregate is **invertible** (SUM, COUNT, AVG) | Maintain running value; **add** entering row, **subtract** exiting row | O(n) |
| Bounded sliding frame, aggregate is **not invertible** (MIN, MAX, or many UDFs) | Can't "subtract" a value from a running max cheaply — engine either recomputes over the window (O(n·w)) or uses an auxiliary structure (e.g., monotonic deque) | O(n·w) worst case, O(n) with specialized structures |

**Practical implication**: a moving-average query using `SUM`/`COUNT` over a sliding frame is cheap; a moving-max/moving-min over the same sliding frame can be **dramatically more expensive** on the same data volume, especially with wide windows. If you need a moving max/min at scale, consider alternate formulations (e.g., bucketed pre-aggregation, or a monotonic-deque based custom implementation) rather than a naive sliding frame.

Spark concretely implements this with distinct physical frame classes (`UnboundedWindowFunctionFrame`, `UnboundedPrecedingWindowFunctionFrame`, `UnboundedFollowingWindowFunctionFrame`, `SlidingWindowFunctionFrame`, `OffsetWindowFunctionFrame`) — the planner picks the class based on your frame spec, and that choice is exactly what determines whether you get the O(n) or O(n·w) code path.

### 3.3 Partitioning cost and skew — the #1 real-world failure mode

Step 1 above (shuffle by `PARTITION BY` key) means: **the maximum single-partition size determines your slowest task.** If your `PARTITION BY` key has a skewed distribution (e.g., `customer_id` where one customer/bot account has 40% of all rows, or a NULL bucket absorbing unmatched keys), that one task:

- Gets a disproportionate share of data,
- Has to sort a huge amount of data in memory (or spill to disk),
- Becomes the straggler that the whole job waits on.

**Diagnosis**: in Spark UI, look for one task in the `Window`/`Sort` stage taking 10-100x longer than the median task, or a single executor spilling large amounts to disk.

**Mitigations**:
- **Salting**: split a skewed key into sub-buckets (`key || '_' || (row_hash % N)`), compute partial aggregates per salted bucket, then combine — useful for pre-aggregation, less directly applicable to ranking functions that need global ordering, but very effective for `SUM`/`COUNT` style windows.
- Pre-filter obviously irrelevant rows (dead/test accounts, nulls) before windowing.
- If there's truly a single dominant key, consider isolating it and processing separately.

**No `PARTITION BY` at all is a special, extreme case of skew** — every row goes into one logical partition, forcing a full single-node sort. `ROW_NUMBER() OVER (ORDER BY ts)` with no partition key on a billion-row table effectively serializes the computation onto one task. This is a very common accidental performance bug — always ask "should this window be partitioned by something?" before removing the clause.

### 3.4 Filtering "before" vs "after" a window function

Because of the logical order (Part 2.3), a naive query computes the window aggregate over **all rows that survive `WHERE`**, then filters afterward:

```sql
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_ts DESC) rn
  FROM events
  WHERE event_date >= '2024-01-01'   -- pushed down, reduces input to window
)
SELECT * FROM ranked WHERE rn = 1;
```

Two performance principles follow:
1. **Push down every filter you can into the `WHERE` before the window CTE** — this reduces the volume that must be shuffled/sorted. Filters that reference the *output* of the window (like `rn = 1`) cannot be pushed down and always execute after.
2. Query optimizers (Spark's Catalyst, Postgres planner) **can** push down predicates that don't depend on the window output through the window operator in many cases, but they **cannot** push down predicates on the window's own output column — always verify with `EXPLAIN`.

### 3.5 Multiple window specs in one query — sort/shuffle multiplication

```sql
SELECT
  SUM(amount) OVER (PARTITION BY cust_id ORDER BY ts)          AS running_total,
  RANK()      OVER (PARTITION BY region  ORDER BY amount DESC) AS region_rank
FROM txns;
```

These two window functions have **different partition keys**, so the engine must do **two separate shuffle+sort passes**. If you have many window functions, group them by identical `(PARTITION BY, ORDER BY)` specs — functions sharing a spec can be computed in a **single pass** with one sort. Reordering/consolidating window specs (or splitting into separate CTEs joined back only when truly needed) is a legitimate, high-value optimization on wide analytical queries.

**How to check**: in Spark, `df.explain(true)` — count the number of `Exchange` (shuffle) and `Sort` nodes preceding `Window` nodes. In Postgres, `EXPLAIN ANALYZE` — count `WindowAgg` nodes and their child `Sort` nodes.

### 3.6 Co-partitioning to avoid unnecessary shuffles

If your data is already bucketed/partitioned by the window's `PARTITION BY` key at rest (e.g., a Spark table bucketed by `cust_id`, or pre-sorted within files), the engine can skip the shuffle step entirely and go straight to a (potentially local) sort — a major cost reduction. This is why, in pipelines with heavy window-function usage on a stable key (e.g., `user_id` sessionization jobs run daily), it's often worth **persisting an intermediate table bucketed/sorted by that key** rather than re-shuffling raw data every run.

### 3.7 Memory & spilling

Even with incremental aggregation, the engine must **buffer at least the current frame's rows** (and for `RANGE`/whole-partition frames, potentially the *entire partition*) in memory to serve the sequential scan. Symptoms of insufficient memory:

- Spark: spill events in the stage for the `Window` operator, visible in Spark UI (`Shuffle Spill (Memory)` / `(Disk)`).
- Postgres: `EXPLAIN (ANALYZE, BUFFERS)` shows disk-based sort/materialize when `work_mem` is exceeded.

Large partitions (from skew) combined with unbounded/whole-partition frames are the classic combination that triggers spill-to-disk and dramatic slowdowns.

---

## Part 4 — Real-World Data Engineering Patterns

### 4.1 Deduplication / "latest record per key" (CDC pipelines)

The most common production use of window functions — collapsing change-data-capture events to current state:

```sql
WITH ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY updated_at DESC, _ingest_seq DESC) rn
  FROM raw_cdc_events
)
SELECT * FROM ranked WHERE rn = 1;
```

**Reasoning trap**: use `ROW_NUMBER`, not `RANK`/`DENSE_RANK` — if two events tie on `updated_at`, `RANK` would keep *both* as rank 1, breaking the "exactly one row per key" guarantee. Always include a deterministic tiebreaker column (ingestion sequence number, offset) — floating timestamps from distributed sources frequently collide.

### 4.2 Top-N per group — RANK vs DENSE_RANK vs ROW_NUMBER

```sql
SELECT * FROM (
  SELECT *, DENSE_RANK() OVER (PARTITION BY category ORDER BY revenue DESC) rnk
  FROM products
) WHERE rnk <= 3;
```

- `ROW_NUMBER`: always exactly N rows per group; ties broken arbitrarily (unless you add a tiebreaker) — use when you need a strict count regardless of ties.
- `RANK`: ties share the same rank, but *skips* subsequent rank numbers (1,1,3) — "top 3" may return more than 3 rows.
- `DENSE_RANK`: ties share rank, no gaps (1,1,2) — "top 3" means "top 3 distinct revenue values," may still return more than 3 rows if there are ties.

Picking the wrong one silently changes "top 3" semantics — this is a frequent source of subtly wrong business reports.

### 4.3 Sessionization (gap-and-island problem)

Classic pattern in clickstream/event pipelines: group events into sessions when the gap between consecutive events exceeds a threshold.

```sql
WITH flagged AS (
  SELECT *,
         CASE WHEN event_ts - LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts)
                   > INTERVAL '30' MINUTE
              OR LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts) IS NULL
              THEN 1 ELSE 0 END AS new_session_flag
  FROM events
)
SELECT *,
       SUM(new_session_flag) OVER (PARTITION BY user_id ORDER BY event_ts
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
FROM flagged;
```

This composes two window function passes: `LAG` to detect a gap, then a **cumulative sum of a boolean flag** to turn "gap markers" into monotonically increasing session IDs — a foundational technique for the general "islands" problem (contiguous runs of a condition).

**Performance note**: this requires the same `(PARTITION BY user_id, ORDER BY event_ts)` spec for both window calls, so a good optimizer computes it in one sorted pass — verify in the plan that there's a single `Sort`/`Exchange` for both.

### 4.4 Moving averages / time-based smoothing (RANGE with intervals)

```sql
SELECT day, revenue,
       AVG(revenue) OVER (ORDER BY day
                          RANGE BETWEEN INTERVAL '6' DAY PRECEDING AND CURRENT ROW) AS trailing_7d_avg
FROM daily_revenue;
```

Using `RANGE` with an interval (rather than `ROWS BETWEEN 6 PRECEDING`) is the *correct* choice when there can be **missing days** — `ROWS 6 PRECEDING` would grab the last 6 *existing* rows even if they span a much longer calendar period, silently producing a misleading "7-day" average over a gappy dataset.

### 4.5 Percent-of-total / share

```sql
SELECT region, sales,
       sales / SUM(sales) OVER () AS pct_of_total,          -- whole-dataset share
       sales / SUM(sales) OVER (PARTITION BY country) AS pct_of_country
FROM regional_sales;
```
`SUM(sales) OVER ()` (no `PARTITION BY`, no `ORDER BY`) — the whole-partition default frame applies, computed once and broadcast; cheap, single pass, no sort even required since there's no `ORDER BY`.

### 4.6 Slowly Changing Dimension change detection

```sql
SELECT *,
       CASE WHEN status <> LAG(status) OVER (PARTITION BY entity_id ORDER BY effective_date)
            THEN 1 ELSE 0 END AS status_changed
FROM scd_history;
```

`LAG`/`LEAD` are themselves window functions with an implicit `ROWS BETWEEN 1 PRECEDING/FOLLOWING AND 1 PRECEDING/FOLLOWING`-like offset behavior — cheap, O(n), but still require the same sort-by-partition-and-order cost as any other window function; don't assume they're "free" just because the frame is tiny.

### 4.7 Streaming caveat

In streaming engines (Spark Structured Streaming, Flink), unbounded window functions over an infinite input are only well-defined with **watermarking** and bounded time windows — you cannot express an arbitrary `PARTITION BY key ORDER BY event_time` running total over a truly unbounded stream without a watermark strategy, because the engine needs to know when it's safe to emit a "final" value and release state. Late-arriving data beyond the watermark is dropped or handled via update-mode semantics — a real constraint that doesn't exist in batch SQL.

---

## Part 5 — Reading Execution Plans (Checklist)

When you see a slow window-function query, check the plan for:

1. **Number of `Exchange`/shuffle nodes** — one per distinct `PARTITION BY` spec (or more if the query has other shuffles from joins). Can any be eliminated by pre-bucketing data or unifying window specs?
2. **Number of `Sort` nodes** and whether they're redundant (sorting by a spec that a previous operator already produced).
3. **Task/partition size skew** in the stage metrics — max vs median task duration and shuffle-read size.
4. **Spill metrics** (memory→disk) on the window/sort stage — indicates frame or partition sizes exceeding available memory.
5. **Which `WindowFunctionFrame`/window-agg strategy** was chosen (where visible) — confirms whether you're on the cheap incremental path or the expensive recompute path (relevant for MIN/MAX/UDFs on sliding frames).
6. **Predicate placement** — confirm filters that don't depend on window output appear *before* the window operator (pushed down), not after.

---

## Part 6 — Pitfall Summary (Quick Reference)

| Pitfall | Why it happens | Fix |
|---|---|---|
| Running total instead of group total | Default frame activates once `ORDER BY` is added | Remove `ORDER BY` or use explicit `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` |
| Duplicated totals on tied order-by values | `RANGE` treats ties as one peer group | Use `ROWS` with a deterministic tiebreaker |
| `WHERE rn = 1` fails | Window functions evaluate after `WHERE`, in the `SELECT` phase | Wrap in CTE/subquery, filter outside |
| "Top N" returns more than N rows | Used `RANK`/`DENSE_RANK` where ties expand the result | Use `ROW_NUMBER` with a tiebreaker if exact count is required |
| Job stuck on one straggler task | Skewed `PARTITION BY` key or missing `PARTITION BY` entirely | Salting, pre-filtering, isolate hot keys, always partition when possible |
| Moving max/min unexpectedly slow at scale | Non-invertible aggregate on a bounded sliding frame forces recompute per row | Reformulate (bucketed pre-agg) or accept O(n·w) for small `w` only |
| Query slower after adding a second window function | Different `PARTITION BY`/`ORDER BY` spec triggers a second shuffle+sort | Unify specs where business logic allows, or verify cost is acceptable |
| "7-day average" wrong on sparse data | `ROWS N PRECEDING` used instead of `RANGE INTERVAL` | Use `RANGE BETWEEN INTERVAL 'N' DAY PRECEDING AND CURRENT ROW` |

---

## Part 7 — Hands-On Exercises

Work these against a real engine (Postgres or Spark SQL) and **inspect the execution plan**, not just the result.

1. **Frame reasoning**: Build a table with duplicate `order_date` values per `customer_id`. Write a running total with `RANGE` (default) and with explicit `ROWS`. Explain the differing outputs in your own words before running it, then confirm.

2. **Skew diagnosis**: Generate a synthetic dataset where one key has 40% of rows. Run `ROW_NUMBER() OVER (PARTITION BY key ORDER BY ts)` on it. Capture the Spark UI stage metrics (or Postgres `EXPLAIN ANALYZE` timing) and identify the skewed task. Apply salting to a `SUM` aggregation version of the same problem and compare.

3. **Plan comparison — invertible vs non-invertible aggregate**: On the same dataset and same sliding frame (`ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING`), compare execution time/plan for `SUM(...)` vs `MAX(...)`. Explain the difference using the incremental-aggregation model from Part 3.2.

4. **Spec consolidation**: Write a query with 4 window functions using 2 distinct `(PARTITION BY, ORDER BY)` specs, deliberately interleaved in a non-adjacent way in the `SELECT` list. Check the plan for the number of sort/shuffle stages. Then rewrite grouping functions by shared spec and confirm whether the plan changes.

5. **Filter pushdown**: Write a two-CTE query: first CTE computes a window function over an unfiltered table; second filters on a column unrelated to the window output. Check whether the planner pushes that filter below the window operator. Then move the filter into the base `WHERE` explicitly and compare plans/costs.

6. **Gap-and-islands**: Given a table of machine status events (`machine_id, status, ts`), write a query that assigns a contiguous "downtime episode id" to consecutive `status = 'DOWN'` rows per machine, and compute the duration of each downtime episode. This forces you to combine `LAG`, conditional flags, and cumulative `SUM` — the same composite pattern as sessionization.

7. **Top-N correctness**: Using a dataset with intentional ties in the ranking column, implement "top 3 per category" three ways (`ROW_NUMBER`, `RANK`, `DENSE_RANK`) and produce row counts per category for each. Explain any category where the three approaches disagree.