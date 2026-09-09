

| Feature | Shuffle Hash Join | Sort Merge Join |
| :--- | :--- | :--- |
| **Data Ordering** | Unsorted data; builds an in-memory hash table on the build-side partition. | Sorts both datasets on the join key within each partition before joining. |
| **Memory Usage** | High memory pressure; entire build-side partition must fit into RAM to build the hash table (may spill to disk if too large). | Lower and more stable memory usage; streams sorted data together using a two-pointer merge approach without keeping a full hash table in RAM. |
| **Default Preference** | Used less frequently in modern Spark (disabled or fallback by default unless configured). | Spark's default and preferred join strategy for large datasets (`spark.sql.join.preferSortMergeJoin = true`). |