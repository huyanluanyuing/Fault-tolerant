# Fault-Tolerant Key-Value Store (ZooKeeper-based)

> **ECE 751: Distributed Computing | University of Waterloo**
> **🏆 Grade: 100% / 100 (Perfect Score)**

![Java](https://img.shields.io/badge/Java-21-007396?style=flat-square&logo=java)
![ZooKeeper](https://img.shields.io/badge/Apache_ZooKeeper-3.9.1-F58025?style=flat-square&logo=apache)
![Curator](https://img.shields.io/badge/Apache_Curator-5.9.0-D12328?style=flat-square&logo=apache)
![Thrift](https://img.shields.io/badge/Apache_Thrift-RPC-231F20?style=flat-square)

## 📖 Overview

This project implements a strongly consistent, fault-tolerant **Key-Value Storage System** capable of surviving node failures without data loss or consistency violations. It uses **Primary-Backup Replication** managed by **Apache ZooKeeper** to ensure high availability and linearizability.

The system was rigorously tested against complex failure scenarios (e.g., cascading crashes, network partitions, port reuse) and achieved a **perfect score (100%)** for maintaining strict linearizability under high-concurrency workloads.

## 🏗 Architecture

The system consists of three main components working in coordination:

1.  **Storage Nodes (Servers)**:
    * **Primary**: Handles all user `put` and `get` requests. Synchronizes state to the backup before acknowledging the client to ensure consistency.
    * **Backup**: Passively replicates data from the primary. Promoted to Primary automatically upon failure detection.
2.  **Coordination Service (ZooKeeper)**:
    * Uses **Ephemeral Sequential Znodes** for leader election and failure detection.
    * Manages cluster membership and notifies clients/backups of topology changes via **Watches**.
3.  **Client**:
    * Automatically discovers the active Primary via ZooKeeper.
    * Transparently redirects requests during failover events.

## ✨ Key Features & Technical Challenges

### 1. Strong Consistency (Linearizability)
* **Correctness**: Passes rigorous linearizability checks where execution histories are validated against sequential specifications, even with incomplete operations caused by crashes.
* **Synchronous Replication**: Ensures that a write operation is only acknowledged after it has been persisted on both the Primary and the Backup.

### 2. Automatic Failover & Recovery
* **Failure Detection**: Uses ZooKeeper ephemeral nodes to detect process crashes instantly.
* **Role Promotion**: The Backup node automatically promotes itself to Primary when the current Primary fails.
* **State Transfer**: New Backup nodes automatically fetch the full dataset from the Primary upon joining to ensure eventual consistency.

### 3. Robustness Handling
The solution handles extreme edge cases described in the coursework:
* **Rapid Failures**: Handles scenarios where nodes crash and restart repeatedly with short intervals, ensuring the backup has time to sync data.
* **Port Reuse**: Correctly identifies stale Primary nodes when a restarted process reuses the same IP:Port but lacks the latest data.
* **Znode Race Conditions**: Handles "Ghost" Znodes where a crashed node's ephemeral node persists temporarily due to session timeout delays.

## 🛠 Tech Stack

* **Language**: Java 21
* **Coordination**: Apache ZooKeeper 3.9.1
* **Client Library**: Apache Curator 5.9.0 (for robust ZK interactions)
* **RPC Framework**: Apache Thrift 0.22.0
* **Testing**: Custom Linearizability Checker (Java)
