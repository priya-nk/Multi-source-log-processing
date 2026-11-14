# 1. Data Flow: From Parser to Deduplication
- As we saw in the corrected pipeline, there is no queue between the parser pool and the deduplication stage. Instead, the data flows via direct function execution:

- Normalization: Parser worker threads pull raw log lines from Queue 1, strip and normalize them into a uniform LogEntry structure (standardizing timestamps, log levels, host sources, and message bodies).

- Fingerprinting: Immediately after normalization, the parser thread computes a 64-bit MurmurHash3 fingerprint over the log's canonical identity keys (timestamp + host_source + message). This condenses a bulky log string into a compact 8-byte integer (uint64_t) stored inside the LogEntry::fingerprint field.

- Direct Handoff: Instead of pushing the entry into a queue, the parser thread directly calls the deduplication engine function (ShardedDedup::is_duplicate()), passing the log entry and its fingerprint.


# 2. How Deduplication Uses the Hash Filter (Lock-Striped Sharded Set)
- If the engine used a single global hash set protected by one mutex, all parser threads would stall waiting for their turn to check the hash table, creating a severe performance bottleneck. To solve this, the deduplication engine uses a Lock-Striped Sharded Hash Set.   
datastructures_algo.md

### How the Sharding Works
- The hash set memory is partitioned into an array of 
N independent shards (typically 16 shards).   
datastructures_algo.md

- Each shard contains its own independent mutex (std::mutex) and its own local hash set (std::unordered_set<uint64_t>).   
datastructures_algo.md

``` C++
struct Shard {
    std::mutex mtx;
    std::unordered_set<uint64_t> fingerprints;
};
std::vector<Shard> shards(16);
```

### Step-by-Step Execution of is_duplicate()
- When a parser thread submits a log's 64-bit fingerprint to the deduplication filter, the following steps execute in microseconds:

- Modulo Partitioning (Shard Selection):
The engine determines which shard owns this fingerprint using modulo arithmetic:

- shard_index=fingerprint(mod16)
- Fine-Grained Locking:
The thread locks only the single mutex dedicated to shards[shard_index]. The other 15 shards remain completely unlocked and accessible to other parser threads simultaneously.

- Lookup & Filter Check:
The thread searches the shard's local std::unordered_set for the fingerprint:

- If found (Duplicate): The thread unlocks the shard, increments an atomic duplicates_dropped counter, and discards the record.   

- If not found (Unique): The thread inserts the fingerprint into the shard's set, unlocks the shard, and safely pushes the unique LogEntry into Queue 3 for the Disk Writer.   


You can be completely sure because shard placement is deterministic, not random or searched.

A specific log's fingerprint will always map to the exact same shard, every single time. Here is why it is mathematically impossible for the same log to land in a different shard:

### 1. The Shard Index is Calculated via Modulo Math
When a parser thread processes a log, it computes its 64-bit MurmurHash3 fingerprint. To find out which shard owns that fingerprint, it applies a strict mathematical formula:

$$\text{shard\_index} = \text{fingerprint} \pmod{16}$$

Because the fingerprint of an identical log string is always identical, and the number of shards ($16$) is constant, the result of that formula is always the exact same number.

* If a log's fingerprint hashes to Shard 5 today, it will hash to Shard 5 tomorrow, next week, and every time it is ever processed.

### 2. Why This Eliminates Uncertainty
* **No Searching Required:** The parser thread doesn't need to search all 16 shards to see if a log has been seen before. It calculates the target shard index instantly using the math above.
* **Exclusive Ownership:** That fingerprint lives only inside that specific shard's hash set. No other shard contains it or knows about it.

This is why lock-striping works so efficiently: because logs are deterministically partitioned by their hash, different parser threads can work concurrently without ever stepping on each other's toes or needing to check multiple shards.