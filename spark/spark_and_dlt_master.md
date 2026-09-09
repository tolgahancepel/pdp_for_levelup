

# Advanced Apache Spark & Delta Live Tables (DLT): Masterclass Study Guide

Welcome, Data Engineer! As your mentor, I have structured this comprehensive study material based on your feedback: you have a solid grasp of fundamental Spark operations, shuffle concepts, and high-level optimizations (salting, broadcast joins, predicate pushdown). However, to transition to a senior level, you need to master **Spark internals, catalyst execution, memory management, advanced troubleshooting**, and **Delta Live Tables (DLT) architecture** for enterprise-grade Lakehouse ETL.

---

## Module 1: Spark Internals & Catalyst Optimizer Deep Dive

To know *when* to apply an optimization, you must understand how Spark turns your high-level DataFrame code into physical execution plans.

### 1.1 The Lifecycle of a Spark Query
1. **Unparsed Logical Plan**: Spark builds an Abstract Syntax Tree (AST) from your DataFrame transformations (e.g., `.filter()`, `.join()`).
2. **Analyzed Logical Plan**: Spark checks the Catalyst Catalog to resolve table and column names, data types, and functions.
3. **Optimized Logical Plan**: The **Catalyst Optimizer** applies standard rule-based optimizations (e.g., constant folding, projection pruning, predicate pushdown).
4. **Physical Plans**: Multiple physical execution strategies are generated based on cost and available algorithms.
5. **Cost-Based Optimizer (CBO)**: Estimates costs (IO, CPU, network shuffle) using statistics collected via `ANALYZE TABLE tbl COMPUTE STATISTICS FOR COLUMNS`.
6. **Code Generation (Whole-Stage Code Generation)**: Spark collapses entire operator trees into single, highly optimized Java bytecode loops, eliminating virtual function call overhead and leveraging CPU cache (JIT compilation).

### 1.2 Catalyst Optimization Rules You Must Know
* **Predicate Pushdown**: Filters are pushed as close to the data source as possible (e.g., Parquet file readers) to minimize disk I/O and network transfer. *Limitation*: UDFs or non-deterministic functions (like `rand()`, `current_timestamp()`) on filter columns often block predicate pushdown.
* **Column Pruning**: Reads only the columns referenced in your query from columnar storage formats (Parquet, ORC, Delta), avoiding wide-row deserialization overhead.
* **Constant Folding**: Evaluates constant expressions at compile time (e.g., transforms `WHERE 1 = 1` or `WHERE salary > 1000 + 5000` into pre-calculated constants).

---

## Module 2: Advanced Spark Memory Management & Troubleshooting OOMs

Most Spark failures stem from improper memory configuration or data skew. Understanding the execution memory layout is critical.

### 2.1 Spark Executor Memory Architecture
Spark divides executor memory (`spark.executor.memory`) into:
* **Reserved Memory**: 300MB reserved for internal system overhead.
* **Spark Memory**: Divided into **Storage** (caching DataFrames/broadcasts) and **Execution** (shuffles, joins, aggregations). 
  * *Dynamic Boundary*: Storage and Execution can borrow memory from each other. If Execution needs memory held by Storage, Storage is evicted to disk or dropped.
* **User Memory**: Used for user-defined data structures, internal metadata, and UDF object overhead (default ~40% of remaining memory after reserved).

### 2.2 Diagnosing and Fixing Out-Of-Memory (OOM) Errors

| OOM Type | Root Cause | Diagnosis | Remediation |
| :--- | :--- | :--- | :--- |
| **Driver OOM (`java.lang.OutOfMemoryError: Java heap space`)** | Collecting too much data to the driver via `.collect()`, `.toLocalIterator()`, or massive broadcast variables. | Check driver logs for large collection calls or massive partition metadata accumulation. | Avoid `.collect()`. Use `.take(n)` or write to storage. Increase driver memory (`spark.driver.memory`). |
| **Executor OOM (Container killed by YARN/K8s for exceeding memory)** | Data skew during shuffles/joins, or unspillable memory usage (large Pandas UDFs, huge Python worker objects). | Look for specific tasks taking 10x longer or failing with exit code 137 in the Spark UI stage summary. | Apply **Salting** for skew, increase partitions (`spark.sql.shuffle.partitions`), or tune memory fractions (`spark.memory.fraction`). |
| **GC Overhead Limit Exceeded** | Too much garbage collection due to massive creation of small Java objects in executor heap. | GC time exceeds 98% of total execution time in Spark UI task metrics. | Optimize code to avoid object thrashing, increase executor memory, or use off-heap memory (`spark.memory.offHeap.enabled`). |

---

## Module 3: Advanced Optimization Scenarios & Decision Frameworks

When should you apply specific optimizations? Use this decision matrix.

### 3.1 Scenario-Based Decision Guide

```
[Is the join table small enough to fit comfortably in executor memory (< 100-200MB default)?]
  ├── YES ──> Use Broadcast Hash Join (BHJ) (`broadcast(df)`) [Avoids Shuffle!]
  └── NO  ──> [Are both datasets already sorted and bucketed on the join key?]
                ├── YES ──> Use Sort-Merge Join (SMJ) [Avoids Shuffle overhead!]
                └── NO  ──> [Is there severe data skew on the join keys (e.g. NULLs or high-frequency keys)?]
                              ├── YES ──> Apply Salting (isolate skewed keys, duplicate small table)
                              └── NO  ──> Spark defaults to Shuffle Hash Join or Sort-Merge Join (Tune shuffle partitions)
```

### 3.2 Deep Dive: When *Not* to Broadcast
While `broadcast()` eliminates expensive shuffles, broadcasting an excessively large DataFrame leads to:
1. **Driver OOM**: The driver must fetch the entire table from executors before broadcasting it to all worker nodes.
2. **Executor OOM**: Executors run out of Storage memory trying to cache the broadcasted table across task slots.
*Rule of thumb*: Only broadcast tables smaller than `spark.sql.autoBroadcastJoinThreshold` (default 10MB, safely tunable up to 100-500MB depending on executor memory sizing).

### 3.3 Managing Data Skew: Beyond Basic Salting
Data skew causes straggler tasks where 95% of tasks finish in seconds, while 1 task processes 80% of the data.
* **The Salting Technique**:
  1. Add a random salt prefix (e.g., `floor(rand() * N)`) to the join key of the skewed DataFrame.
  2. Expand the matching side of the join by replicating rows across all $N$ salt values (`explode` or cross-join with a range table of size $N$).
  3. Perform the join on `(salted_key, original_key)`.
  4. Drop the salt column.
* **Adaptive Query Execution (AQE) Skew Join Optimization**:
  * In modern Spark (3.x+), enable `spark.sql.adaptive.enabled = true` and `spark.sql.adaptive.skewJoin.enabled = true`. AQE automatically detects partitioned skew at runtime and splits large partitions into smaller sub-partitions dynamically without manual salting!

---

## Module 4: Delta Live Tables (DLT) vs. Traditional ETL

Traditional Spark ETL requires manual orchestration (Airflow/Databricks Workflows), manual state management, complex checkpointing for streaming, and explicit table maintenance (VACUUM, OPTIMIZE). **Delta Live Tables (DLT)** is a declarative framework that automates Lakehouse pipeline management.

### 4.1 Architectural Comparison

| Dimension | Traditional Spark ETL | Delta Live Tables (DLT) |
| :--- | :--- | :--- |
| **Paradigm** | Imperative (Procedural steps: read, transform, write) | Declarative (Define *what* data looks like via expectations; DLT manages execution order) |
| **Dependency Management** | Manual DAG creation in orchestration tools (Airflow, Jobs API) | Automatic lineage graph resolution based on table/view references (`@dlt.table`) |
| **Data Quality & Governance** | Manual logging or filtering (try-catch, custom error tables) | Built-in **Expectations** (`@dlt.expect_or_drop`, `@dlt.expect_or_fail`, `@dlt.expect`) with automated metrics logging |
| **Pipeline Maintenance** | Manual VACUUM, OPTIMIZE, Z-ORDER scheduling | Automated maintenance, compaction, and checkpoint management |
| **Auto-Scaling / Infrastructure** | Manual cluster tuning | Serverless compute or optimized auto-scaling for streaming and batch execution |

### 4.2 DLT Medallion Architecture Implementation Pattern
In DLT, you define pipelines using Python or SQL decorators. Here is a production-grade Python DLT example showcasing the Medallion architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold):

```python
import dlt
from pyspark.sql.functions import col, current_timestamp, expr

# --- BRONZE LAYER: Ingest raw streaming data (e.g., JSON from Kafka/Cloud Storage) ---
@dlt.table(
    name="orders_bronze",
    comment="Raw ingested customer orders from cloud storage landing zone",
    table_properties={"quality": "bronze"}
)
def orders_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/mnt/landing/orders/")
        .withColumn("ingest_timestamp", current_timestamp())
    )

# --- SILVER LAYER: Clean, validate, and enforce data quality constraints ---
@dlt.table(
    name="orders_silver",
    comment="Cleaned and validated orders with data quality expectations enforced",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_fail("positive_amount", "amount > 0.0")
def orders_silver():
    return (
        dlt.read_stream("orders_bronze")
        .select(
            col("order_id"),
            col("customer_id"),
            col("amount").cast("double"),
            col("order_date"),
            col("ingest_timestamp")
        )
    )

# --- GOLD LAYER: Aggregated business metrics for BI and Analytics ---
@dlt.table(
    name="customer_daily_sales_gold",
    comment="Daily aggregated sales metrics per customer",
    table_properties={"quality": "gold"}
)
def customer_daily_sales_gold():
    return (
        dlt.read("orders_silver")
        .groupBy("customer_id", "order_date")
        .agg(
            expr("sum(amount) as total_daily_spend"),
            expr("count(order_id) as total_orders")
        )
    )
```

### 4.3 Key DLT Features to Master
* **Expectations Action Modes**:
  * `expect`: Records warning metrics in the event log but retains the bad record.
  * `expect_or_drop`: Drops invalid records silently (ideal for noisy telemetry or log data).
  * `expect_or_fail`: Halts and fails the pipeline immediately if data violates the constraint (critical for financial/compliance data).
* **Change Data Capture (CDC) with `APPLY CHANGES INTO`**: DLT simplifies SCD Type 1 and SCD Type 2 operations natively using change feeds (e.g., from Debezium or Delta Change Data Feed).

---

## Module 5: Hands-On Scenario Challenge (For Your Practice)

To test your mastery of these concepts, analyze the following real-world scenario:

> **Scenario**: You are managing a Spark job processing 2TB of clickstream logs joined with a 50GB user profile dimension table in Delta Lake. The job frequently crashes with an `Executor OOM (Exit Code 137)` on the join stage. When inspecting the Spark UI, you notice that 3 tasks take 45 minutes while all other 197 tasks finish in 30 seconds. Furthermore, business stakeholders want automated data quality enforcement and line-of-sight tracking without maintaining manual Airflow DAGs.

### Questions to Consider:
1. What two distinct Spark optimization features are causing or failing to handle this OOM?
2. Why would a broadcast join fail in this scenario?
3. How would migrating this batch/streaming workload to Delta Live Tables solve both the performance bottleneck and the orchestration/data quality challenge?

---

*How would you like to proceed? We can dive deeper into Spark UI metrics analysis, walk through a custom CDC implementation in DLT, or discuss your answers to the scenario challenge!*