## Simple Rule First

**Delta NEVER edits an existing parquet file. It only ever does two things:**
1. **Write a brand-new parquet file** (for new/changed data)
2. **Mark old files as removed** in the log (tombstone), if their data is no longer valid

Whether a new file gets created depends on the operation. Let's go through each one.

---

## 1. INSERT — Always creates a new file. Never touches old files.

```sql
INSERT INTO t VALUES (10, 'new');
```

**Before:**
```
part-A.parquet (id 1-5)
part-B.parquet (id 6-9)
```

**After:**
```
part-A.parquet (id 1-5)   ← untouched
part-B.parquet (id 6-9)   ← untouched
part-C.parquet (id 10)    ← NEW FILE
```

Log: `add C` → active files = {A, B, C}

**No existing file is touched. Simple append.**

---

## 2. UPDATE — Creates a new file ONLY for files containing matched rows

```sql
UPDATE t SET val = 'x' WHERE id = 7;
```

id=7 lives inside `part-B.parquet`.

**Before:**
```
part-A.parquet (id 1-5)
part-B.parquet (id 6-9)
```

**After:**
```
part-A.parquet (id 1-5)      ← untouched (no match here)
part-B.parquet (id 6-9)      ← tombstoned (old version)
part-B2.parquet (id 6-9, updated) ← NEW FILE replacing B
```

Log: `remove B, add B2` → active files = {A, B2}

**Key point:** even though only 1 row changed, the *entire file* B gets rewritten as B2 (unless Deletion Vectors are enabled — see below). File A is completely untouched because it had no matching rows.

---

## 3. DELETE — Same behavior as UPDATE

```sql
DELETE FROM t WHERE id = 7;
```

**After:**
```
part-A.parquet (id 1-5)        ← untouched
part-B.parquet (id 6-9)        ← tombstoned
part-B3.parquet (id 6,8,9)     ← NEW FILE, same as B minus row 7
```

Log: `remove B, add B3` → active files = {A, B3}

---

## 4. MERGE — Mix of both: rewrites matched files + adds new files for inserts

```sql
MERGE INTO t USING updates u ON t.id = u.id
WHEN MATCHED THEN UPDATE SET val = u.val
WHEN NOT MATCHED THEN INSERT *;
```

If `updates` has id=7 (matches, in file B) and id=99 (new, no match):

**After:**
```
part-A.parquet (id 1-5)         ← untouched
part-B.parquet                  ← tombstoned
part-B4.parquet (id 6-9 updated)← NEW (replaces B)
part-D.parquet (id 99)          ← NEW (the inserted row)
```

Log: `remove B, add B4, add D` → active files = {A, B4, D}

---

## Quick Reference Table

| Operation | New file created? | Which old files touched? |
|---|---|---|
| INSERT | Yes, always (new data) | None |
| UPDATE | Yes, for any file containing a matched row | Only files with matches → tombstoned |
| DELETE | Yes, for any file containing a matched row (rewritten minus deleted rows) | Only files with matches → tombstoned |
| MERGE | Yes — both for updated files AND newly inserted rows | Only files with matches → tombstoned |

---

## One Exception: Deletion Vectors (newer feature)

If `delta.enableDeletionVectors = true`:

```sql
DELETE FROM t WHERE id = 7;
```

Instead of rewriting all of file B:
```
part-B.parquet          ← STAYS on disk, untouched, still "active"
deletion_vector_xyz.bin ← NEW tiny file: "in part-B.parquet, row for id=7 is deleted"
```

Log: `add deletion_vector reference to B` (no full rewrite of B)

This avoids rewriting the whole file for small deletes — but it's the exception, not the default behavior. Without deletion vectors, **any row-level change means the entire containing file gets rewritten as new.**

---

## The core mental model

> A Delta table is a pile of immutable parquet files. Every write operation either **adds files to the pile** or **replaces some files in the pile with new versions** — it never edits a file that's already there. The log just keeps score of which files in the pile currently count.