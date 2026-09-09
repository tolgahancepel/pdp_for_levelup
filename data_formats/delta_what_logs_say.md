## The JSON log = the "score sheet" that says which files currently count

That's it. That's the whole role.

## Why you need it

Every operation creates new parquet files and leaves old ones sitting on disk (never deleted immediately). After many operations, your folder looks like this:

```
part-A.parquet
part-B.parquet   ← old, replaced
part-B2.parquet  ← replacement for B
part-C.parquet
part-D.parquet   ← old, replaced
part-D2.parquet  ← replacement for D
```

**Just looking at the folder, you cannot tell which files are currently valid.** Is B2 the replacement for B? Is D still needed? A folder listing alone doesn't tell you.

## The JSON log is the answer key

Each JSON commit just records two simple facts per file:

```json
{"add": "part-B2.parquet"}     ← this file is now valid
{"remove": "part-B.parquet"}   ← this file is no longer valid
```

Nothing more. It doesn't store data. It just says **"count this one in, count this one out."**

## Put it together

| Folder has (physically) | Log says | Result: table = |
|---|---|---|
| A, B, B2, C, D, D2 | remove B, add B2, remove D, add D2 | A + B2 + C + D2 |

The log is how you go from "a messy pile of files on disk" → "the exact set of files that make up the table right now."

## One sentence

**Parquet files hold the data. The JSON log holds the list of which files are currently valid.** You need both — data without the log is just a messy folder; the log without data is just a list.