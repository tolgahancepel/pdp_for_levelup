## The problem checkpoints solve

Every operation adds one more JSON file. After 1000 operations, you have 1000 JSON files. To know "which files currently count," you'd have to open and read **all 1000** JSON files, one by one, and tally up all the add/remove entries.

That's slow.

## Checkpoint = a pre-computed answer sheet

A checkpoint just says: **"Here's the final tally, already calculated, as of this version. Don't bother reading the 1000 JSONs before this — I already did the math."**

## Example

**Without checkpoint**, to read the table at version 1000:
```
Open json 1 → remove nothing, add A
Open json 2 → remove nothing, add B
Open json 3 → remove A, add A2
...
Open json 1000 → remove nothing, add Z
```
= 1000 files opened, just to figure out the current file list.

**With a checkpoint at version 990:**
```
Open checkpoint_990.parquet → "current valid files as of v990: {A2, B7, C3, ..., Y}"
Open json 991
Open json 992
...
Open json 1000
```
= 1 checkpoint + 10 JSONs opened, instead of 1000 JSONs.

## What's literally inside the checkpoint

Just a table (rows = files), like:

| file | status |
|---|---|
| A2.parquet | active |
| B7.parquet | active |
| C3.parquet | active |

It's the **already-resolved tally**, saved so nobody has to redo that math from scratch every time.

## One sentence

**A checkpoint is a save-point for the log's bookkeeping — "here's the current file list as of version 990" — so you only replay the few JSONs after it, instead of every JSON since version 0.**