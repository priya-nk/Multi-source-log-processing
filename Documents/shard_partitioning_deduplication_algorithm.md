# Shard Partitioning & Deduplication Algorithm

## 1. What is the Shard Partitioning & Deduplication Algorithm?

The **Shard Partitioning & Deduplication Algorithm** (implemented in `ShardedDedup::is_duplicate()`) is the core mechanism in Stage 3 of the pipeline responsible for identifying and filtering out redundant log entries in real-time.

### The Problem It Solves

As multiple parser worker threads normalize log streams concurrently, duplicate logs frequently arrive from various sources (e.g., retried network packets, identical error states, or redundant telemetry). The engine guarantees that duplicate logs are dropped while unique entries are preserved, all while avoiding global lock contention across multi-core processors.

## 2. Step-by-Step Execution Flow

1. **Input Reception:** The algorithm receives a normalized `LogEntry` containing its precomputed 64-bit MurmurHash3 fingerprint (`uint64_t`) from the parser thread.

2. **Modulo Shard Partitioning:** Instead of using a single global hash set, the algorithm computes the target shard index using strict modulo arithmetic:

$$\text{shard\_index} = \text{fingerprint} \pmod{\text{NUM\_SHARDS}}$$

*(In standard configurations, the memory is partitioned into $16$ independent shards).*

3. **Fine-Grained Locking (Lock Striping):** The thread acquires the `std::mutex` dedicated exclusively to that specific shard (`shards[shard_index].mtx`). The other 15 shards remain completely unlocked and accessible to other threads.

4. **Lookup Check:** The thread searches the shard's local `std::unordered_set<uint64_t>` for the fingerprint:
   * **If Found (Duplicate):** The thread unlocks the shard, increments the atomic `duplicates_dropped` counter, and discards the record.
   * **If Not Found (Unique):** The thread inserts the fingerprint into the shard's set, unlocks the shard, and safely pushes the unique `LogEntry` into Queue 3 for sequential disk writing.

## 3. Why Global Lock Hash Sets Are Rejected

A naive implementation uses a single `std::unordered_set` protected by one global mutex. Under high-throughput workloads, this architecture causes a global lock contention bottleneck:

* Worker threads spend the vast majority of their CPU clock cycles stalled, waiting in line for exclusive access to the single global lock.
* This completely serializes execution, wasting multi-core hardware capabilities and reducing throughput to a fraction of a single core's capacity.

By partitioning the hash set into lock-striped shards, the engine allows up to $N$ threads (e.g., 16) to perform lookups and insertions simultaneously without colliding.