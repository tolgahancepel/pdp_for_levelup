# Parquet Internals & Delta Lake Transaction Log Mechanics

## Part 1: Parquet File Internals

### The Physical Layout

A Parquet file is organized hierarchically. Getting the terminology precise matters because it drives real performance decisions.

```
Parquet File
└── Row Group 1 (e.g., 128MB of rows)
│   ├── Column Chunk (col A) — contiguous bytes on disk
│   │   ├── Page 1 (e.g., 1MB, dictionary-encoded)
│   │   ├── Page 2
│   │   └── Page 3
│   ├── Column Chunk (col B)
│   └── Column Chunk (col C)
├── Row Group 2
│   └── ... same structure
└── Footer (schema, row group stats, offsets)
```

**Correcting a common inaccuracy:** there's no such thing as a "column group" in Parquet's spec. The actual units are:

- **Row group**: A horizontal slice of the table (a batch of rows). Default target size is often 128MB–1GB depending on writer settings.
- **Column chunk**: Within a row group, all values for one column, stored contiguously. This is what makes Parquet columnar — column chunks let you skip reading columns you don't need.
- **Page**: The smallest unit of encoding/compression within a column chunk. Pages contain the actual encoded values plus optional statistics.

### Why This Layout Matters Practically

1. **Column pruning** happens at the column chunk level — reading `SELECT a FROM t` means the reader seeks to column chunk A's byte range in each row group and skips B and C entirely, using the footer's offset metadata.

2. **Predicate/row-group pruning** relies on statistics (min/max, null count) stored per column chunk in the footer. If your query filters `WHERE date = '2024-01-01'` and a row group's min/max for `date` doesn't overlap, the whole row group is skipped without reading any data. This is why **sorting/clustering data on filter columns before writing** dramatically improves prune rates — random ordering means every row group's min/max spans the whole range, defeating pruning.

3. **Row group sizing is a real tradeoff:**
   - Too small (e.g., many 8MB row groups) → excessive footer metadata, more open/seek overhead, worse compression ratios (less data per dictionary/encoding context), poor pruning granularity.
   - Too large (e.g., a single 5GB row group for a 5GB file) → can't parallelize reads within a file per row group, memory pressure when reading a full row group into memory, less benefit from row-group-level stats pruning.
   - Common sweet spot: 128MB–512MB row groups for cloud object storage (fewer, larger GET requests are cheaper than many small ones on S3/ADLS).

4. **Page size** affects fine-grained skipping (page-level stats, added in newer Parquet spec versions) and decoding overhead — smaller pages mean more decode calls but finer stats. Usually left at defaults (1MB) unless you're doing deep tuning.

5. **Dictionary encoding** is chosen per column chunk based on cardinality. Low-cardinality string/int columns get dictionary-encoded automatically by most writers — this is why column order and type choice affect file size more than people expect.

---

## Part 2: Delta Lake Transaction Log Mechanics

### The Core Model

Delta doesn't rewrite Parquet files for updates — it's an **append-only log of actions** referencing immutable Parquet files. This is the root of both its power and its storage-growth behavior.

```
delta_table/
├── part-0001.parquet          ← data file 1
├── part-0002.parquet          ← data file 2
└── _delta_log/
    ├── 00000000000000000000.json   ← commit 0: ADD file1, file2
    ├── 00000000000000000001.json   ← commit 1: ADD file3, REMOVE file1
    ├── 00000000000000000002.json   ← commit 2: ADD file4
    ├── ...
    ├── 00000000000000000010.checkpoint.parquet  ← periodic snapshot
    └── _last_checkpoint
```

Each JSON commit file contains an ordered list of **actions**:
- `add` — a new Parquet file is now part of the table (with stats, partition values)
- `remove` — a file is logically removed (marked with a tombstone, **not deleted from disk immediately**)
- `metaData`, `protocol`, `commitInfo`, `txn` — schema, versioning, and metadata actions

**The table's current state = replay all actions from version 0 (or last checkpoint) forward**, keeping only files that are `add`ed and not subsequently `remove`d.

### Why This Directly Causes Storage Growth (the part usually explained wrong)

This is the key mechanic to get right:

1. **UPDATE/DELETE/MERGE never edit existing Parquet files in place.** Parquet is immutable. An UPDATE that touches 1 row in a 100MB file (with default settings, no deletion vectors) rewrites the *entire file* it belongs to as a new file, then issues a `remove` action for the old file and an `add` action for the new one.

2. **The old file is not deleted from disk.** The `remove` action is a *tombstone* — it tells the log "this file is no longer logically part of the table as of this version," but the physical file stays on storage. This is essential for:
   - **Time travel** (`VERSION AS OF`, `TIMESTAMP AS OF`) — querying an old version requires the old physical files to still exist.
   - **Concurrent readers** who started a query against an older snapshot before the commit landed.

3. **Storage growth = (physical file accumulation from rewrites) + (log file accumulation)**, and both need explicit maintenance:
   - **`VACUUM`** physically deletes tombstoned files older than the retention threshold (default 7 days). Without running VACUUM, every UPDATE/DELETE/MERGE/OPTIMIZE leaves its predecessor files sitting on disk indefinitely. This is the single most common cause of "why is my Delta table 5x larger than the data" — nobody's running VACUUM, or it's on a schedule that doesn't fit write frequency.
   - Lowering `delta.deletedFileRetentionDuration` reduces the safe time-travel/concurrency window but shrinks storage faster if you VACUUM aggressively — there's a real tradeoff here, not a free lunch.

4. **Checkpoints add their own overhead but bound log growth, not data growth.** Delta writes a checkpoint (a Parquet snapshot of the log state) every N commits (default 10) so readers don't have to replay every JSON file since version 0. Checkpoints don't affect data file storage — they're a metadata optimization. Without them, log replay time grows with commit count, which is a *query latency* problem, not a storage-growth-of-data problem (though the JSON files themselves do accumulate — also subject to log cleanup, `delta.logRetentionDuration`, default 30 days).

5. **Small-file accumulation from frequent low-volume writes (e.g., streaming micro-batches)** is a separate but related growth vector — each commit can add small files, and without `OPTIMIZE` (compaction) these accumulate as many small `add`ed files, hurting both storage efficiency and read performance (small file problem — more open/seek overhead per query).

6. **Deletion vectors** (newer feature, Delta 2.3+/DBR 12.2+) change this calculus for DELETE/UPDATE: instead of rewriting the whole file, a small auxiliary file marks specific rows as deleted, avoiding full-file rewrite. This significantly reduces storage churn for selective row-level deletes but adds a bit of read-time overhead (merging deletion vectors) and its own compaction lifecycle (deletion vectors get "compacted away" during OPTIMIZE).

### Practical Consequence Checklist

| Symptom | Likely Cause | Fix |
|---|---|---|
| Table storage far exceeds `DESCRIBE DETAIL` size estimate / logical data size | Tombstoned files not vacuumed | Run `VACUUM`, check retention settings |
| Slow queries despite small logical data | Many small files from frequent writes | `OPTIMIZE` (compaction), tune write batch size |
| Slow table load / first query latency | Long log replay, checkpoint not triggering | Check checkpoint interval, `_last_checkpoint` health |
| Time travel to old version fails | VACUUM already ran past that version | Nothing to fix — it's expected; align retention window with time-travel needs |
| MERGE/UPDATE workloads causing disproportionate storage churn | Full-file rewrites on small updates | Enable deletion vectors, or repartition to reduce file size touched per update |

---

### The One-Sentence Version of Each

- **Parquet**: row groups → column chunks → pages; layout drives pruning and I/O efficiency, and there's no such thing as a "column group."
- **Delta**: the table is a *log of add/remove actions over immutable files*, so updates always create new files and tombstone old ones — storage doesn't shrink until you `VACUUM`.

Want me to go deeper on any one piece — e.g., how `OPTIMIZE`/Z-ordering interacts with row group layout, or how checkpoint + log replay actually resolves the current file list on read?