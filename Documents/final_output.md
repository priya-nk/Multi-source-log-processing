# Multi-Source Log Normalization & High-Performance Analytics Engine

A multi-threaded C++ log processing, normalization, and deduplication engine designed to ingest heterogeneous log streams concurrently from multiple remote TCP sources. The engine normalizes raw data into a unified schema, filters duplicates in real-time using lock-striped sharded hash sets, and sequentially writes clean records to disk under strict bounded-memory constraints.

## 1. System Architecture & Conceptual Data Flow

The engine is engineered as a multi-stage concurrent pipeline connected via thread-safe bounded queues. If downstream processing slows down, bounded queues enforce thread blocking, which propagates network-level backpressure via TCP flow control to remote senders, preventing application memory exhaustion (OOM crashes).

```
[Source A (JSON)]   \
[Source B (Syslog)]  --> [ Receiver ] ---> (Queue 1: Lock-Free Ring Buffer) ---> [ Parser Pool ]
[Source C (Apache)] /         |                                                       |
                              v (Partial line on drop)                                v (Malformed / Bad format)
                       [ Quarantine ]                                          [ Quarantine ]
                                                                                      |
                                                                                      v
                                                                           [ Sharded Dedup Engine ]
                                                                           (Lock-Striped Hash Set)
                                                                                      |
                                                                                      v
                                                                        (Queue 3: Bounded Queue)
                                                                                      |
                                                                                      v
                                                                            [ Single Writer Thread ]
                                                                                      |
                                                                                      v
                                                                             [ Output File ]

```

## 2. Core Data Structures & Concurrency Components

### 1. Lock-Free Circular Ring Buffer (`LockFreeBoundedQueue<T>`)

* **Usage:** High-throughput pipeline buffers between processing stages (Queue 1, Queue 2, Queue 3).

* **Under the Hood:** Single-Producer Single-Consumer (SPSC) / Multi-Producer Multi-Consumer (MPMC) lock-free circular ring buffer utilizing C++ `std::atomic` cache-aligned head/tail pointers and memory fences.

* **Backpressure Mechanism:** Eliminates OS-level `std::mutex` context switching. When full, producers yield/spin safely before falling back to condition-variable wait states, preserving low latency and propagating network backpressure upstream.

### 2. Sharded Hash Set (Lock-Striped Exact Filter)

* **Usage:** Real-time duplicate verification executed concurrently across parallel threads without bottlenecking on a single global lock.

* **Under the Hood:** An array of $N$ shards (e.g., 16 independent `std::unordered_set<uint64_t>` instances), each protected by its own dedicated `std::mutex`.

* **Routing:** Modulo arithmetic (`fingerprint % NUM_SHARDS`) isolates lock acquisition to isolated memory buckets.

### 3. Sliding Byte Buffer (`std::vector<char>`)

* **Usage:** Maintained per active TCP socket connection to handle partial network packet framing.

* **Under the Hood:** Dynamic linear byte buffer retaining incomplete fragments split across TCP boundaries (`\n`).

### 4. Unified Log Schema (`struct LogEntry`)

* **Usage:** Standardized structural representation of log records passing through the pipeline.

* **Under the Hood:** Memory-aligned struct:

  ```cpp
  struct LogEntry {
      std::string timestamp;   // ISO-8601 UTC timestamp
      std::string log_level;   // INFO, WARN, ERROR, DEBUG
      std::string host_source; // Originating remote IP / identifier
      std::string message;     // Normalized log payload
      uint64_t fingerprint;    // Precomputed 64-bit hash key
  };
  ```

## 3. Core Algorithms & Optimization Techniques

### 1. Byte-Stream Framing Algorithm (Scanner)

* Scans incoming TCP byte streams for newline delimiters (`\n`).

* Extracts complete lines instantly and pushes them into **Queue 1**.

* *Disconnect Handling:* If a socket terminates mid-line, the leftover partial fragment is routed to `quarantine.log` with the reason `incomplete_frame_connection_reset`.

### 2. Format Detection & Schema Normalization

* Parallel parser worker threads pull raw strings from Queue 1.

* Automatically detects heterogeneous formats (JSON, Apache, Syslog, Key-Value pairs).

* Normalizes timestamps to standard ISO UTC strings and constructs a `LogEntry`.

* Unparseable or malformed inputs bypass main processing and write directly to `quarantine.log` with explicit tags (e.g., `reason: "unknown_format"`).

### 3. MurmurHash3 / Fast Non-Cryptographic Hashing

* Converts normalized log keys (`timestamp + host_source + message`) into uniform 64-bit unsigned integer signatures (`uint64_t`).

* Offers high hash distribution balance and near-zero CPU latency compared to cryptographic hash functions (SHA/MD5).

### 4. Deduplication Pipeline

1. **Fingerprint Calculation:** Calculate 64-bit MurmurHash3 signature of the normalized record.

2. **Sharded Hash Check:** Lock only the specific shard (`fingerprint % 16`). Drops exact duplicates and increments `duplicates_dropped` (`std::atomic<uint64_t>`).

3. Unique entries are pushed directly to **Queue 3**.

### 5. TCP Flow Control & Network-Level Backpressure

* Cascading backpressure model:
  

  $$
  \text{Slow Writer} \longrightarrow \text{Queue 3 Full} \longrightarrow \text{Dedup Blocks} \longrightarrow \text{Queue 2 Full} \longrightarrow \text{Parsers Block} \longrightarrow \text{Queue 1 Full} \longrightarrow \text{Receiver Blocks}
  $$

* When the Receiver thread stops issuing socket `read()` / `recv()` calls, the OS kernel TCP Receive Buffer fills up.

* Operating System sends a `TCP Zero Window` signal to the remote sender, pausing transmission at the remote host without memory leakage or record loss.

## 4. Key Specification & Implementation Rules

1. **Dashboard Out of Scope:** Per Gate 0 guidelines, web/GUI dashboards are strictly out of scope. Validation is conducted via terminal execution metrics, atomic counters, and standard programmatic test scripts.

2. **Lossless Ingestion Definition:** "Without dropping records" means zero internal queue drops or out-of-memory crashes. Network backpressure safely pauses senders during heavy load.

3. **Quarantine Handling:** Malformed logs, unknown formats, and disconnected partial lines append to `quarantine.log` alongside structured failure metadata.

4. **Conservation Invariant (Verification Target):**
   

   $$
   \text{Total Lines Received} = \text{Lines Written to Output} + \text{Duplicates Dropped} + \text{Quarantined Lines}
   $$

## 5. File & Directory Structure

```text
├── include/
│   ├── LockFreeBoundedQueue.h  # Atomic circular ring buffer (SPSC/MPMC)
│   ├── BoundedBlockingQueue.h  # Fallback condition-variable queue
│   ├── LogEntry.h              # Unified schema struct
│   ├── Receiver.h              # Socket listener & stream framer
│   ├── ParserPool.h            # Multi-threaded format normalizer
│   ├── ShardedDedup.h          # Lock-striped exact hash filter
│   └── DiskWriter.h            # Sequential file flusher thread
├── src/
│   ├── Receiver.cpp
│   ├── ParserPool.cpp
│   ├── ShardedDedup.cpp
│   ├── DiskWriter.cpp
│   └── main.cpp                # Orchestrator & signal handling setup
├── tests/
│   └── log_simulator.py        # Multi-source concurrent log generator
├── Makefile / CMakeLists.txt
└── README.md
```

## 6. Building and Running

### Prerequisites

* C++17 or C++20 compliant compiler (`g++` or `clang++`)

* POSIX Threads (`pthread`)

* CMake 3.14+ (optional)

### Build Commands

```bash
mkdir -p build && cd build
cmake ..
make -j4
```

### Execution

```bash
./log_processor_engine --port 8080 --output normalized_output.log --quarantine quarantine.log
```