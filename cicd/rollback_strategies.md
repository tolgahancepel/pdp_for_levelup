# Rollback Strategies in Data Engineering CI/CD Pipelines

## Why This Matters: The Data Engineering Rollback Problem

Traditional software rollback = redeploy old binary. **Data pipeline rollback is harder** because you're managing three coupled things simultaneously:

| Layer | What can break | Example |
|---|---|---|
| **Code** | Transformation logic, DAG definitions | Bad join logic in a Spark job |
| **Schema** | Table structure, contracts with consumers | Column type change breaks a BI dashboard |
| **Data/State** | Actual data already written, offsets, checkpoints | A backfill corrupts 6 months of history |

A software rollback only needs to fix (1). A data pipeline rollback often needs to fix all three — and data already consumed downstream may need reprocessing. This is why **idempotency** and **backward-compatible schema evolution** are prerequisites for every strategy below.

---

## 1. Blue-Green Deployments

### Core Concept
Maintain two identical environments (**Blue** = current live, **Green** = new candidate). Traffic/reads switch atomically from Blue to Green once Green is validated. Rollback = flip the pointer back.

### How It Translates to Data Engineering

Unlike a web server, "environment" in DE can mean a **table, dataset, topic, or cluster**. The atomic switch is the critical property — you're not doing gradual traffic shifting (that's canary), you're doing an instant cutover with an instant undo.

**Pattern A — Table/View Swap (Data Warehouse)**
```sql
-- Build the new version fully, in isolation
CREATE TABLE sales_mart_green AS
SELECT * FROM staging.sales_transformed_v2;

-- Validate (row counts, dbt tests, reconciliation against sales_mart_blue)

-- Atomic swap (metadata-only operation, near-zero downtime)
ALTER TABLE sales_mart RENAME TO sales_mart_blue_old;
ALTER TABLE sales_mart_green RENAME TO sales_mart;

-- Rollback if issues found post-swap
ALTER TABLE sales_mart RENAME TO sales_mart_broken;
ALTER TABLE sales_mart_blue_old RENAME TO sales_mart;
```
In **Snowflake**, use zero-copy clones instead of full rebuilds — cheap, instant, storage-efficient:
```sql
CREATE TABLE sales_mart_green CLONE sales_mart;
-- apply changes to clone, validate, then swap
ALTER TABLE sales_mart SWAP WITH sales_mart_green;
```

**Pattern B — Dual Pipeline / Topic Swap (Streaming)**
Run old and new consumer logic in parallel, writing to `enriched_events_blue` and `enriched_events_green` topics/tables. Downstream reader config points to one alias. Rollback = repoint the alias, no reprocessing needed since both were live simultaneously.

**Pattern C — Cluster/Environment Level (Orchestration)**
Two full Airflow/Spark environments; a load balancer or DNS/service mesh entry decides which environment's endpoints are "production." Used when infra config itself (library versions, cluster settings) is the risky change, not just code.

### Key Properties
- **Cutover is instantaneous and reversible** — this is the whole value proposition.
- Requires **double the infrastructure/storage** during transition (cost tradeoff).
- Green must be built from data as fresh as Blue at cutover time — a **sync/drift window** exists if source data changes during Green's build. Mitigate with CDC replay or freezing writes briefly during swap.
- Best for: **schema changes, full table rebuilds, major transformation rewrites** where you need a clean, testable "before/after."

---

## 2. Version Rollbacks

### Core Concept
Every deployable artifact (code, schema, config, container image) is versioned and tagged. Rollback = redeploy a previously known-good version. This is **reactive** (something is already broken in prod) vs blue-green which is **preventive** (validate before it becomes prod).

### The Four Things You Must Version Independently

**1. Code / DAG version**
```bash
git tag -a v2.3.1 -m "stable release before risky refactor"
git push origin v2.3.1
# Rollback:
git revert <bad-commit-sha>   # or
git checkout v2.3.1 -- dags/orders_pipeline.py
```
Airflow 2.x+ supports **DAG Versioning** natively — each parsed DAG file version is stored, and the UI lets you inspect/compare historical DAG structure runs against. Pin deployments via container image tags (`orders-dag:2.3.1`), never `:latest`.

**2. Schema version (the hard one)**
Use a migration tool with **explicit up/down migrations** (Flyway, Alembic, Liquibase, or dbt's contract + version workflow):
```sql
-- V12__add_customer_segment.sql (up)
ALTER TABLE customers ADD COLUMN segment VARCHAR(20);

-- V12__add_customer_segment_rollback.sql (down)
ALTER TABLE customers DROP COLUMN segment;
```
**Golden rule:** schema changes should be *additive and backward-compatible* whenever possible (add nullable column, don't rename/drop) — this makes rollback trivial and avoids breaking consumers who haven't updated yet. Destructive changes need a two-phase deprecation window.

**3. Data version (snapshot rollback)**
This is the layer plain code-rollback tools ignore. Use storage formats with native time travel:
```sql
-- Delta Lake
RESTORE TABLE orders TO VERSION AS OF 45;
-- or
RESTORE TABLE orders TO TIMESTAMP AS OF '2024-01-15 08:00:00';

-- Iceberg
CALL system.rollback_to_snapshot('db.orders', 8734659345345L);
```
Without a transactional table format, "rolling back data" means restoring from backup/snapshot or replaying source events — much slower and often lossy. This is a major architectural argument for adopting Delta/Iceberg/Hudi in any pipeline where rollback matters.

**4. Config/parameter version**
API keys, connection strings, transformation parameters (e.g., a "lookback window = 7 days" config) should be versioned alongside code in something like Airflow Variables backed by git, or a config table with an `effective_from` timestamp — never mutated in place.

### Rollback Depth Decision
| Symptom | What to roll back |
|---|---|
| Bug in transformation logic, data itself still fine | Code/DAG version only |
| Schema change broke downstream consumer | Schema version (down migration) |
| Bad data already written to production tables | Data version (time travel / restore) + code |
| All three | Full pipeline rollback — see combined checklist below |

---

## 3. Feature Flags

### Core Concept
Decouple **deployment** from **release**. New logic ships to production disabled/dark, then is turned on gradually via a flag lookup, without a redeploy. Rollback = flip a boolean, not a pipeline redeploy.

### Data Engineering Applications

**A. Logic toggles in transformation code**
```python
from feature_flags import get_flag

def transform_orders(df):
    if get_flag("use_new_discount_logic", context={"env": "prod"}):
        return apply_discount_v2(df)
    return apply_discount_v1(df)
```
Flag source can be a config table, Airflow Variable, or a proper service (LaunchDarkly, Unleash, AWS AppConfig). For pipelines, **avoid runtime SaaS calls per-record** — resolve the flag once per job run for performance and reproducibility (log which flag value was used per run for auditability).

**B. Percentage/segment-based rollout (canary via flags)**
```python
def should_use_new_logic(customer_id, rollout_pct=10):
    return (hash(customer_id) % 100) < rollout_pct
```
Used to validate a new enrichment or scoring model against a small % of production traffic before full rollout — critical for ML feature pipelines and pricing logic where errors are costly.

**C. Shadow/Dark writes**
Run new logic in parallel, write output to a *separate* table, never expose it downstream. Compare against production output offline. This is a feature-flag-gated blue-green hybrid — zero risk to consumers, full production-scale validation.
```python
if get_flag("shadow_mode_new_pricing"):
    new_output.write.saveAsTable("pricing_shadow")
prod_output.write.saveAsTable("pricing_live")  # unaffected
```

**D. Kill switches for pipeline stages**
```python
if not get_flag("enable_enrichment_step"):
    logging.warning("Enrichment disabled via flag — skipping")
    return df
```
Lets on-call engineers instantly disable a misbehaving stage (e.g., an external API enrichment call that started failing) without touching the DAG.

### Feature Flags vs Blue-Green vs Version Rollback

| | Grain of control | Speed | Requires redeploy? |
|---|---|---|---|
| **Feature flag** | Per-function/per-record logic branch | Seconds | No |
| **Blue-green** | Whole dataset/environment | Seconds (swap) but slow to prepare | No (swap), Yes (to build green) |
| **Version rollback** | Code/schema/data artifact | Minutes | Yes |

They are **complementary, not competing** — a mature pipeline uses all three at different layers simultaneously (see next section).

---

## 4. Designing a Unified CI/CD Pipeline

### Pipeline Stages

```
1. Build & Unit Test
   └─ lint, unit test transformation logic, validate schema definitions (schema-as-code)

2. Deploy to Staging (isolated env, synthetic or sampled prod data)
   └─ integration tests, dbt tests / Great Expectations suite

3. Deploy to "Green" in Production (dark — feature-flagged OFF)
   └─ new code/tables exist in prod but flag keeps traffic on "Blue"

4. Shadow Run / Canary (flag-controlled, small % or parallel write)
   └─ compare Green output vs Blue output: row counts, checksums, distribution drift
   └─ automated data quality gate: FAIL → auto-disable flag, alert, stop

5. Progressive Rollout (flag % increase: 5% → 25% → 100%)
   └─ monitor SLA, freshness, error rate, downstream consumer health at each step

6. Promote (Blue-Green Swap)
   └─ atomic rename/alias switch; old Blue retained as immediate rollback target

7. Post-promotion Monitoring Window
   └─ automated rollback trigger active for N hours (e.g., data quality SLA breach)

8. Archive / Cleanup
   └─ tag the promoted version, schedule old Blue for retention-window deletion
```

### Sample CI/CD YAML Skeleton (GitLab/GitHub Actions style)

```yaml
stages:
  - test
  - deploy_staging
  - deploy_dark
  - canary
  - promote
  - rollback   # triggered, not sequential

test:
  script:
    - pytest tests/unit
    - dbt test --target staging

deploy_dark:
  script:
    - dbt run --target prod --vars '{deploy_mode: green}'   # builds green tables/clones
    - flag-cli set enable_new_pipeline=false                 # stays dark

canary:
  script:
    - flag-cli set enable_new_pipeline=true --rollout=5%
    - python scripts/compare_blue_green.py --threshold 0.001
  rules:
    - if: $CI_JOB_STATUS == 'failed'
      # triggers rollback job automatically

promote:
  script:
    - flag-cli set enable_new_pipeline=true --rollout=100%
    - sql-cli execute "ALTER TABLE sales_mart SWAP WITH sales_mart_green"
    - git tag "prod-$(date +%Y%m%d%H%M)"
  when: manual   # human approval gate for full promotion

rollback:
  script:
    - flag-cli set enable_new_pipeline=false          # instant kill
    - sql-cli execute "ALTER TABLE sales_mart SWAP WITH sales_mart_blue_old"  # blue-green undo
    - dbt run --target prod --vars '{version: previous_stable_tag}'          # version rollback if code-level
  when: on_failure
```

### The Rollback Decision Gate (automation logic)

```python
def evaluate_rollback(metrics: dict) -> bool:
    checks = [
        metrics["row_count_delta_pct"] < 1.0,
        metrics["null_rate_new"] <= metrics["null_rate_baseline"] * 1.1,
        metrics["pipeline_latency_p95"] < SLA_THRESHOLD,
        metrics["downstream_consumer_error_rate"] == 0,
    ]
    return not all(checks)   # True = trigger rollback
```
This gate should run automatically after each rollout percentage increase — this is what makes rollback *proactive* instead of relying on a human noticing a broken dashboard.

---

## 5. Real-World Case Studies

**Case 1 — E-commerce schema migration (Blue-Green + Version Rollback)**
A team needs to change `order_status` from a free-text column to an enum-backed integer FK. They clone the table in Snowflake, run the migration + backfill on the clone, validate with dbt tests comparing row-level equivalence against the original, then swap. When a downstream BI tool breaks because it expected text, they swap back within 2 minutes — no data loss, no redeploy of the BI layer, buying time to fix the consumer.

**Case 2 — Streaming enrichment logic change (Feature Flag + Shadow Mode)**
A fraud-detection team changes the scoring algorithm in a Flink/Spark Structured Streaming job. Rather than a direct swap, they run the new scorer in shadow mode, writing to `fraud_scores_shadow` for 48 hours while production continues using `fraud_scores_v1`. Offline comparison shows a 3% divergence in edge cases; they fix and re-validate before flipping the flag to route 10% of live traffic, then 100%.

**Case 3 — ML feature store deployment (Blue-Green at the store level)**
A feature store computes rolling aggregates for a recommendation model. Recomputing all historical features for a logic change is expensive, so they build the new feature set into a "green" table, run model backtests against both blue and green feature sets, and only redirect the serving layer's alias once backtest metrics are equal-or-better. Rollback is an alias change, avoiding any recomputation.

**Case 4 — Airflow DAG regression (Version Rollback)**
A refactored DAG introduces a bug where a task incorrectly marks itself as `success` despite an upstream API failure, silently skipping data. Alerting catches missing rows same day. On-call reverts to the git-tagged previous DAG version via CI redeploy, then triggers a backfill DAG run for the missed date using the restored logic — codifying why **idempotent, re-runnable tasks** are a prerequisite for any version rollback to actually fix downstream damage.

**Case 5 — Backfill gone wrong (Data Version Rollback via Delta time travel)**
An engineer runs a "fix" backfill script with an off-by-one date bug, overwriting 3 days of a Delta table with duplicated data. Because the table is Delta Lake, `RESTORE TABLE ... TO VERSION AS OF <n>` reverts to the pre-backfill snapshot in seconds. Without Delta/Iceberg, this incident would have required restoring from a nightly backup and losing intraday data — this case is often cited as the single strongest ROI argument for adopting transactional table formats.

---

## 6. Common Pitfalls & Anti-Patterns

- **Non-idempotent transformations.** If reprocessing the same input twice produces different output (e.g., using `CURRENT_TIMESTAMP` inside a transform, or `INSERT` instead of `MERGE`), no rollback strategy can safely re-run the pipeline.
- **Rollback tested only in theory.** Run periodic "rollback drills" (chaos engineering for data pipelines) — actually execute the rollback path in staging regularly, not just document it.
- **Feature flag debt.** Flags left in code long after rollout completes become permanent branching complexity and a hidden source of bugs. Set an expiry/cleanup ticket at flag creation time.
- **Schema drift during blue-green window.** If Blue keeps receiving writes while Green is being built/validated, Green is stale at swap time. Use CDC to replay the delta into Green just before swap, or freeze writes briefly.
- **Ignoring downstream consumers during schema rollback.** Rolling a schema back to fix your pipeline can re-break a consumer who already adapted to the new schema — always check consumer contracts before a schema-level rollback, not just producer-side tests.
- **Conflating "redeploy old code" with "fix the data."** Version rollback fixes future runs; it does **not** retroactively fix already-corrupted data. Always pair code rollback with an explicit backfill/data-restore decision.
- **No automated gate, human bottleneck.** If rollback requires paging someone at 3am to manually run commands, MTTR balloons. Automate the rollback trigger (flag flip, table swap) and reserve manual approval only for the *promotion* step.

---

## 7. Decision Framework — Which Strategy When

| Situation | Primary strategy | Secondary support |
|---|---|---|
| New transformation logic, uncertain correctness | Feature flag (shadow → canary) | Version rollback as final safety net |
| Schema change / full table rewrite | Blue-green (clone-and-swap) | Down migration scripted regardless |
| Bug shipped, data still clean | Version rollback (code only) | — |
| Bug shipped, data corrupted | Version rollback + Data snapshot restore | Backfill plan |
| Infra/cluster config change (Spark version, library upgrade) | Blue-green (environment level) | Feature flag if app-level toggle exists |
| ML model / scoring logic change | Feature flag (shadow mode + % rollout) | Blue-green at serving layer |

---

## 8. Practical Checklist for Any DE Rollback Design

- [ ] Is the transformation idempotent (safe to re-run)?
- [ ] Is the table format transactional / snapshot-capable (Delta, Iceberg, Hudi)?
- [ ] Does every schema migration have a tested "down" script?
- [ ] Are code deployments pinned to immutable versions/tags, never `latest`?
- [ ] Is there an automated data-quality gate that can trigger rollback without a human?
- [ ] Are feature flags resolved once-per-run (not per-record) and logged for auditability?
- [ ] Has the rollback path itself been executed in a drill, not just documented?
- [ ] Do downstream consumers have a contract test that runs before schema-level rollback?
- [ ] Is there a defined cleanup/expiry policy for feature flags and old blue-green artifacts?