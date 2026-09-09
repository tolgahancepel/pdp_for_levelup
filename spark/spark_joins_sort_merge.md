


![Image](file:url::files/9viwBaUCDrGQtRzBYNjQ7xPbiaA4koq2ya8KS1QjEskoBcN5ZGBQd2XG52rx2YDpDD/appdata/gpt-image-2-2026-04-21/images/6a953653a2083d6b1eba64319d3b3069e16c4a75e75382c338298c1b79e7bc2c.png)

---

### How Sort Merge Join (SMJ) Works in Apache Spark

Sort Merge Join is Spark’s most robust and default join strategy for large datasets. It operates in **three main phases**: **Shuffle**, **Sort**, and **Merge**. 

Below is a comprehensive, step-by-step breakdown using example data inside executor nodes.

---

### Step 1: Shuffle Phase (Partitioning by Join Key)
Before joining, Spark ensures that rows with the *same join key* from both datasets (`Dataset A` and `Dataset B`) end up on the *same executor node*. 
* It applies a hashing function to the join key.
* Rows with matching hash codes are sent (shuffled) to the corresponding executor partition.

#### Example Data in Executor Nodes After Shuffle:

| **Executor Node 1** | **Executor Node 2** |
| :--- | :--- |
| **Dataset A (Orders):**<br>• `(dept_id: 10, order_id: 101)`<br>• `(dept_id: 10, order_id: 102)`<br>• `(dept_id: 20, order_id: 103)` | **Dataset A (Orders):**<br>• `(dept_id: 30, order_id: 104)` |
| **Dataset B (Departments):**<br>• `(dept_id: 10, name: "HR")`<br>• `(dept_id: 20, name: "IT")` | **Dataset B (Departments):**<br>• `(dept_id: 30, name: "Finance")` |

---

### Step 2: Sort Phase (Local Sorting)
Once data arrives at each executor node, Spark sorts both datasets independently **in-memory (or spilled to disk if memory is insufficient)** based on the join key (`dept_id`).

#### Data State After Sorting (Within Each Executor):

* **Executor Node 1:**
  * **Dataset A (Sorted):** `(10, 101)`, `(10, 102)`, `(20, 103)`
  * **Dataset B (Sorted):** `(10, "HR")`, `(20, "IT")`

* **Executor Node 2:**
  * **Dataset A (Sorted):** `(30, 104)`
  * **Dataset B (Sorted):** `(30, "Finance")`

---

### Step 3: Merge Join Phase (Two-Pointer Scan)
Now that both datasets on every executor are sorted by the join key, Spark performs a **single pass (O(N + M) linear scan)** using two pointers—one for Dataset A and one for Dataset B:
1. Compare the keys pointed to by both pointers.
2. If the keys **match**, output the joined row and advance pointer(s).
3. If Dataset A's key is **smaller**, advance Dataset A's pointer.
4. If Dataset B's key is **smaller**, advance Dataset B's pointer.

#### Final Joined Output Generated Across Nodes:
* **Executor Node 1 Output:**
  * `(dept_id: 10, order_id: 101, name: "HR")`
  * `(dept_id: 10, order_id: 102, name: "HR")`
  * `(dept_id: 20, order_id: 103, name: "IT")`
* **Executor Node 2 Output:**
  * `(dept_id: 30, order_id: 104, name: "Finance")`

---

### Summary of Pros & Cons
* ✅ **Scalable:** Handles massive datasets that cannot fit entirely in memory by spilling sort buffers to disk.
* ⚠️ **Performance Overhead:** The shuffle and sort phases incur heavy network I/O and CPU costs. 
* 💡 **Tip:** If one table is small enough to fit in executor memory, consider using a **Broadcast Hash Join (BHJ)** to bypass the shuffle and sort phases entirely.