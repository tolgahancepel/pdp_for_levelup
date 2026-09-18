# AWS vs GCP for Data Engineering: A Design Decision Framework

## How to Use This Guide

This isn't a feature checklist—those become outdated and don't teach you anything transferable. Instead, this guide organizes around **decision points**: moments in real data engineering work where the platform's underlying philosophy forces you down different architectural paths. Understanding *why* these differences exist will help you reason about any cloud platform, not just these two.

---

## Part 1: The Core Philosophical Divide

Before comparing services, understand this foundational difference—it explains almost every downstream design decision.

### AWS: Infrastructure Building Blocks
AWS gives you granular, composable primitives. You assemble pipelines like Lego bricks—more control, more configuration, more responsibility.

### GCP: Opinionated, Managed Systems
GCP tends to give you fewer, more integrated services that make more decisions for you—often based on Google's internal infrastructure (Borg, Dremel, Spanner lineage).

**Why this matters for design:**
- AWS rewards teams who want fine-grained control and are willing to manage complexity (IAM policies, VPC configs, service orchestration).
- GCP rewards teams who want to move fast and are willing to accept "the Google way" of doing things.

**Real consequence:** A startup with 2 data engineers will often ship faster on GCP (BigQuery + Dataflow) because there's less infrastructure to reason about. A large enterprise with a dedicated platform team may prefer AWS because they *want* that granular control to enforce custom security/compliance boundaries.

---

## Part 2: Storage — The Foundation of Every Pipeline

### S3 vs GCS: Similar API, Different Mental Model

| Aspect | AWS S3 | GCP GCS |
|---|---|---|
| Consistency | Strong (as of Dec 2020) | Strong |
| Storage classes | Explicit lifecycle rules per class | Autoclass (automatic tiering) |
| IAM model | Bucket policies + IAM policies (two systems) | Unified IAM (simpler mental model) |

**Design impact:**

The **dual permission system** in S3 (bucket policies AND IAM policies AND ACLs) is a common source of production incidents—accidental public exposure, or conversely, access denials that take hours to debug because you have to check three places.

> **Real-world scenario:** A data engineer needs to grant a Lambda function read access to a bucket used by 5 other services. In AWS, you must reconcile: (1) the Lambda's execution role IAM policy, (2) the bucket policy, and (3) any bucket-level object ACLs. In GCP, it's one IAM binding on the bucket or even the object. This isn't just "convenience"—it's a real reduction in **attack surface for misconfiguration**, one of the top causes of cloud data breaches.

**Design decision point:** If your organization has strict compliance requirements (e.g., needing resource-level AND identity-level policies for defense-in-depth), AWS's complexity is a *feature*. If your team is small and misconfiguration risk from complexity outweighs the benefit of granularity, GCP's model reduces operational risk.

---

## Part 3: The Data Warehouse Decision — Redshift vs BigQuery

This is the single most consequential choice for analytics workloads, and the architectural difference is deep, not cosmetic.

### Redshift: Provisioned, Cluster-Based
- You choose node types and count.
- Storage and compute are coupled (with RA3 nodes partially decoupling this).
- You manage vacuum, sort keys, distribution keys.

### BigQuery: Serverless, Fully Decoupled
- No clusters to provision.
- Storage and compute are fully separate—storage is cheap and infinite; compute is billed per-query (or via flat-rate slots).
- No indexes, no vacuum, no distribution keys to manage.

**Design impact — this changes how you architect ingestion:**

```
AWS/Redshift mental model:
"I need to size a cluster for my PEAK load, 
 and I pay for that cluster 24/7 whether 
 queries run or not."

GCP/BigQuery mental model:
"I pay per query (or per reserved slot pool),
 so I need to think about query efficiency 
 and cost per scan, not cluster utilization."
```

**Real-life use case — cost modeling:**

Imagine a company with bursty analytics: heavy usage 9am–6pm on weekdays, near-zero on weekends.

- **Redshift:** You either overprovision (pay for idle capacity on weekends) or build automation to pause/resume clusters (added engineering complexity, and cold-start latency when resuming).
- **BigQuery:** Naturally matches this pattern—you pay per query scanned. Zero weekend cost, no pipeline needed to "pause" anything.

**But the reverse case matters too:**

A company running thousands of small, frequent dashboard queries all day:
- **BigQuery on-demand pricing** (per TB scanned) can become unpredictable and expensive if queries aren't well-optimized (e.g., `SELECT *` on huge tables, no partitioning).
- **Redshift** with a properly sized, always-on cluster gives predictable flat costs regardless of query volume.

> **Key design principle:** BigQuery's pricing model *forces* good habits (partitioning, clustering, column pruning) because bad queries are immediately visible in cost. Redshift's pricing model *hides* inefficiency behind the flat cluster cost—until you hit capacity limits.

**Practical engineering consequence:** On BigQuery, you'll invest early in **partitioning by date** and **clustering by common filter columns**, because the cost feedback loop is immediate and painful otherwise. On Redshift, this discipline is easy to defer because the cost signal is delayed (you just see cluster CPU climbing).

---

## Part 4: Orchestration and ETL — Different Defaults

### AWS: Glue (Serverless Spark) + Step Functions + Lambda
### GCP: Dataflow (Apache Beam) + Cloud Composer (managed Airflow)

**The critical difference: the programming model underneath.**

- **AWS Glue** is fundamentally **batch-oriented Spark**, with streaming as an add-on (Glue Streaming, or separately Kinesis).
- **GCP Dataflow** is built on **Apache Beam**, which unifies batch and streaming in a single programming model from the start.

**Design impact:**

```python
# Apache Beam (Dataflow) - the SAME code path handles 
# batch and streaming with minor source/sink swaps

pipeline = beam.Pipeline()
(pipeline 
 | beam.io.ReadFromPubSub(topic=topic)  # streaming source
 # | beam.io.ReadFromText(file_pattern) # OR batch source - same downstream logic
 | beam.Map(transform_fn)
 | beam.io.WriteToBigQuery(table))
```

**Real-world scenario:** A company starts with a nightly batch ETL job (Glue on AWS, or Dataflow batch on GCP) and later needs to convert it to near-real-time.

- **On AWS:** This often means a *rewrite*—moving logic from Glue/Spark batch jobs into a Kinesis + Lambda streaming architecture, because the programming models and APIs are meaningfully different.
- **On GCP:** If the original pipeline was written in Beam, converting to streaming can be a matter of **swapping the I/O connector** and adding windowing logic—the transform logic often stays untouched.

**Design decision point:** If you anticipate migrating from batch to streaming (a *very* common data engineering evolution), choosing Beam/Dataflow from day one on GCP reduces future rewrite cost. On AWS, you'd deliberately choose to write Glue jobs using Spark Structured Streaming patterns from the start if you want this flexibility, since Glue's ecosystem doesn't push you toward unification by default.

---

## Part 5: Streaming — Kinesis vs Pub/Sub vs Dataflow

| Aspect | AWS Kinesis | GCP Pub/Sub |
|---|---|---|
| Model | Shard-based (you manage partition/shard count) | Fully serverless, no shard management |
| Scaling | Manual (or auto-scaling add-on) resharding | Automatic |
| Ordering | Per-shard ordering guaranteed | Ordering only with ordering keys (opt-in) |

**Design impact — capacity planning burden:**

**Kinesis** requires you to think about **shards** as a first-class design concern:
- Each shard = 1MB/s write, 2MB/s read.
- Too few shards → throttling.
- Too many shards → wasted cost and complexity in consumer parallelism.
- Resharding is an operational event you need to plan for and monitor.

**Pub/Sub** has no shard concept exposed to you—Google handles partitioning internally.

> **Real-world scenario:** An e-commerce platform expects a 10x traffic spike during a flash sale.
>
> - **On Kinesis:** You must pre-provision additional shards ahead of the event (or configure auto-scaling with careful tuning) and monitor `IteratorAge` metrics to detect consumer lag. Under-provisioning causes `ProvisionedThroughputExceededException` and data loss risk if retries aren't handled.
> - **On Pub/Sub:** You don't provision anything—the service scales automatically. Your design concern shifts entirely to the **consumer side**: can your downstream processing (Dataflow, Cloud Functions) scale fast enough?

**This reveals a general pattern:** AWS tends to expose scaling knobs (shards, IOPS, provisioned throughput) that give you control but require capacity planning skill. GCP tends to abstract scaling away but reduce your visibility into *why* something is slow (you can't inspect "shard hot-spotting" the same way).

**Design decision point:** If your traffic patterns are highly predictable, Kinesis's explicit shard model lets you optimize cost precisely. If your traffic is unpredictable or spiky (common in growing startups), Pub/Sub's serverless model removes an entire category of on-call incidents (shard exhaustion).

---

## Part 6: IAM and Security Models — A Different Blast Radius

### AWS: Resource-Centric, Highly Granular
- Nearly every resource type has its own policy language nuances.
- Cross-account access via role assumption (`sts:AssumeRole`) is powerful but has a learning curve.

### GCP: Project-Centric, Hierarchical by Default
- Resources live inside Projects, which live inside Folders, which live inside an Organization.
- IAM policies naturally inherit down this hierarchy.

**Design impact:**

```
AWS mental model:
- Account boundary is the primary isolation unit
- You often multi-account by environment (dev/staging/prod accounts)
- Requires AWS Organizations + SCPs for governance at scale

GCP mental model:
- Project boundary is the primary isolation unit  
- Folder hierarchy naturally maps to org structure
  (e.g., Org > Department Folder > Team Folder > Project)
- IAM inheritance means less explicit policy duplication
```

**Real-world scenario — multi-team data platform:**

A company has 5 data teams, each needing isolated BigQuery/Redshift datasets but shared access to a central data lake.

- **On AWS:** Common pattern is separate AWS accounts per team + a central "data lake account," with cross-account IAM roles and Lake Formation for fine-grained table/column permissions. This is powerful but requires deliberate governance tooling (AWS Organizations, SCPs) to avoid sprawl.
- **On GCP:** Common pattern is separate **Projects** per team under a shared Folder, with IAM policies inherited from the Folder level for common permissions (e.g., "everyone in this folder can read the shared dataset"), reducing repeated policy definitions.

**Design decision point:** GCP's hierarchy reduces policy duplication but can make it harder to reason about "who actually has access" without tracing the full inheritance chain—a real audit challenge. AWS's flatter, more explicit model is more auditable per-resource but requires more upfront policy-writing discipline to avoid drift across accounts.

---

## Part 7: Serverless Compute for Data Pipelines — Lambda vs Cloud Functions vs Cloud Run

| Aspect | AWS Lambda | GCP Cloud Run |
|---|---|---|
| Execution model | Function-based, event-triggered | Container-based, HTTP or event-triggered |
| Max timeout | 15 minutes | 60 minutes (configurable) |
| Cold start | Present, mitigated by provisioned concurrency | Present, mitigated by min instances |
| Packaging | Zip/layer or container image | Container image (any language/runtime) |

**Design impact:**

Cloud Run's container-first model means you're not locked into specific runtime versions or the function-handler pattern—you can run *any* containerized process, including existing batch scripts, with minimal rewriting.

> **Real-world scenario:** A team has an existing Python data validation script (using a specific pandas/numpy version) that needs to run on file upload.
>
> - **Lambda:** You need to fit it into the Lambda handler pattern, respect the deployment package size limit (250MB unzipped, or use container images up to 10GB), and manage layer dependencies carefully.
> - **Cloud Run:** You containerize the existing script almost as-is (Dockerfile + your existing code), deploy it, and trigger it via Eventarc on GCS object creation. Less adaptation of existing code required.

**Design decision point:** For teams heavily invested in a specific existing codebase (not written function-handler style), Cloud Run's "just containerize it" model reduces migration friction. For teams building new, small, fast event handlers, Lambda's mature ecosystem (huge number of native event source integrations) is often faster to wire up.

---

## Part 8: Cost Model Philosophy — A Recurring Theme

This deserves its own section because it's a pattern you'll see across *every* service comparison above:

| Pattern | AWS Tendency | GCP Tendency |
|---|---|---|
| Compute pricing | Provisioned capacity (pay for what you reserve) | Consumption-based (pay for what you use) — with reservations optional |
| Committed discounts | Reserved Instances / Savings Plans (1-3yr commit) | Committed Use Discounts (1-3yr) *and* automatic Sustained Use Discounts (no commitment needed) |
| Egress | Complex, varies heavily by service and destination | Simpler, but still a real cost (don't ignore cross-region GCS transfer) |

**The recurring design lesson:** AWS's provisioning-first model rewards **capacity planning skill**—if you know your workload well, you can optimize costs aggressively with Reserved Instances or Savings Plans. GCP's consumption-first model rewards **inherent workload efficiency**—costs naturally track usage, so the platform is more forgiving for teams that *don't* have mature capacity planning practices yet, but it can punish inefficient code/queries directly and immediately.

---

## Part 9: Putting It Together — A Decision Framework

When facing an AWS vs. GCP decision for a specific pipeline component, ask these questions in order:

1. **Do we need fine-grained infrastructure control for compliance/security reasons?**
   → Lean AWS (more explicit knobs, more mature multi-account governance tooling like Lake Formation, SCPs)

2. **Is our team small and do we prioritize velocity over granular control?**
   → Lean GCP (BigQuery, Pub/Sub, Cloud Run reduce operational surface area)

3. **Will this workload evolve from batch to streaming?**
   → Lean GCP/Beam if you want to avoid a future rewrite; otherwise design AWS pipelines with Spark Structured Streaming from day one if flexibility matters

4. **Is our workload usage pattern bursty/unpredictable or steady/predictable?**
   → Bursty → GCP's consumption pricing (BigQuery, Pub/Sub) naturally fits
   → Steady, well-understood → AWS's reserved/provisioned pricing can be cheaper *if* you have the capacity planning discipline

5. **Do we already have deep expertise or existing infrastructure in one ecosystem?**
   → This is often the deciding factor in practice—rewriting working systems for marginal architectural benefit is rarely worth it. Multi-cloud is expensive in complexity; most companies benefit more from mastering one platform deeply than splitting focus.

---

## Key Takeaway

The AWS vs. GCP distinction in data engineering isn't "which has more features"—AWS almost always has more services and more configuration options. The real distinction is: **AWS asks you to assemble your own opinions about architecture; GCP tends to bake its opinions in for you.** Neither is universally better—the right choice depends on whether your team's bottleneck is *lack of control* or *too much complexity to manage*.