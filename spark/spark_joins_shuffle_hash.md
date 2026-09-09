


![Shuffle Hash Join in Spark](files/9viwBaUCDrGQtRzBYNjQ7xPbiaA4koq2ya8KS1QjEskoBcN5ZGBQd2XG52rx2YDpDD/appdata/gpt-image-2-2026-04-21/images/fdf4c9e61049d40c4c3a5a99e6f09334ec38ded7b78388a496619ca0c8a5f80a.png)

***

# How Shuffle Hash Join Works in Spark

A **Shuffle Hash Join** is one of the standard join strategies in Apache Spark. It is used when joining two large datasets where neither dataset is small enough to fit into memory (which would otherwise trigger a Broadcast Hash Join). 

---

## 📋 Example Scenario
Let's join two datasets—**`Orders`** and **`Customers`**—on the common key **`CustomerID`**:

* **`Orders` Table:** Contains purchase records.
* **`Customers` Table:** Contains customer details.

---

## 🔄 Step-by-Step Execution Flow

### Step 1: Partitioning & Shuffling (The Shuffle Phase)
Spark needs to bring rows with the **same join key** from both datasets onto the **same executor node**. 
* Spark applies a hashing function to the join key (`CustomerID`) for every row in both datasets.
* Rows with the exact same hash value are sent over the network to the same target executor partition.

#### 📊 Example Data Distribution across Executors after Shuffling:

| Executor Node 1 | Executor Node 2 |
| :--- | :--- |
| **Orders Partition (Key hashes to Node 1):**<br>• `(OrderID: 101, CustomerID: 10, Item: "Book")`<br>• `(OrderID: 103, CustomerID: 10, Item: "Pen")`<br><br>**Customers Partition (Key hashes to Node 1):**<br>• `(CustomerID: 10, Name: "Alice")` | **Orders Partition (Key hashes to Node 2):**<br>• `(OrderID: 102, CustomerID: 20, Item: "Desk")`<br><br>**Customers Partition (Key hashes to Node 2):**<br>• `(CustomerID: 20, Name: "Bob")` |

---

### Step 2: Building Hash Tables (The Build Phase)
Inside each executor node, Spark selects one of the datasets (usually the smaller one per partition) to build an in-memory **Hash Table** using the join key as the index.

* **On Executor 1:** Builds a hash table for `Customers` where key = `10` points to `(Name: "Alice")`.
* **On Executor 2:** Builds a hash table for `Customers` where key = `20` points to `(Name: "Bob")`.

---

### Step 3: Probing and Joining (The Join Phase)
Spark streams through the rows of the second dataset (the "streamed" side—in this case, `Orders`) partition by partition, probing the hash table built in Step 2.

#### 🔍 Resulting Joined Output per Executor:

* **Executor 1 Result:**
  * `(OrderID: 101, CustomerID: 10, Item: "Book")` ➕ `(Name: "Alice")` $\rightarrow$ `(101, 10, "Book", "Alice")`
  * `(OrderID: 103, CustomerID: 10, Item: "Pen")` ➕ `(Name: "Alice")` $\rightarrow$ `(103, 10, "Pen", "Alice")`

* **Executor 2 Result:**
  * `(OrderID: 102, CustomerID: 20, Item: "Desk")` ➕ `(Name: "Bob")` $\rightarrow$ `(102, 20, "Desk", "Bob")`

---

## ⚙️ Key Characteristics of Shuffle Hash Join
1. **Network Heavy:** Requires shuffling *both* datasets across the cluster over the network, which can cause significant network I/O and disk spill if partitions are skewed.
2. **Memory Intensive:** Requires enough RAM on executors to build hash tables for the partitions. If a hash table exceeds memory limits, Spark spills data to disk.
3. **When does Spark choose it?** Spark falls back to Shuffle Hash Join when datasets are too large to broadcast, but the join keys have a reasonable distribution (and when `spark.sql.join.preferSortMergeJoin` is disabled or Sort-Merge join isn't preferred).