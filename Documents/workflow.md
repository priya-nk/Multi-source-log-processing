Here is a high-level visual breakdown of how a log line travels through the engine, along with the error paths and safety mechanisms.

---

### 1. End-to-End Pipeline Overview

```text
  [ CLIENTS / SOURCES ]
    JSON   Syslog   Apache
      │       │       │
      └───┬───┴───┬───┘
          │ (Raw TCP Byte Streams)
          ▼
┌─────────────────────────┐
│     1. RECEIVER         │ ─── [Partial line on disconnect] ───► 📁 quarantine.log
│  (Sliding Byte Buffer)  │
└───────────┬─────────────┘
            │ Complete lines (\n)
            ▼
    (Queue 1: Ring Buffer)
            │
            ├───────────────┬───────────────┐
            ▼               ▼               ▼
    ┌───────────────┐┌───────────────┐┌───────────────┐
    │ Parser Thread ││ Parser Thread ││ Parser Thread │  (Parser Pool)
    └───────┬───────┘└───────┬───────┘└───────┬───────┘
            │                │                │
            │  [Malformed / Unknown Format]   └─────────────────► 📁 quarantine.log
            ▼
    ┌─────────────────────────────────────────────────┐
    │ 2. SHARDED DEDUPLICATION (Lock-Striped Hash)    │
    │                                                 │
    │   Fingerprint % 16  ──►  Lock Shard [0..15]     │
    └───────────────┬─────────────────────────────────┘
                    │
                    ├─── [Duplicate Found] ────────────► 🗑️ Dropped (Counter + 1)
                    │
                    ▼ (Unique LogEntry)
            (Queue 3: Bounded Queue)
                    │
                    ▼
    ┌─────────────────────────┐
    │ 3. DISK WRITER (Single) │
    └───────────┬─────────────┘
                │
                ▼
      📁 normalized_output.log
```

---

### 2. Step-by-Step Lifecycle of a Log

#### Stage 1: Framing (Stream $\to$ Discrete Lines)
* Multiple remote sources stream raw bytes concurrently into a single receiver socket.
* The **Receiver** buffers incoming fragments until it hits a newline (`\n`).
* **Exit route:** If a client abruptly disconnects mid-transmission, the partial fragment is flushed directly to `quarantine.log`.

```text
TCP Stream:  "...WARN connection reset\n2026-11-10 [INCOMPL..." (Socket Drop!)
                                │                              │
                                ▼                              ▼
                       Queue 1 (Complete)              quarantine.log
```

---

#### Stage 2: Normalization (Text $\to$ Structured Object)
* Parallel worker threads pop raw text from **Queue 1**.
* The worker detects the format (JSON, Syslog, Apache) and normalizes it into a standard struct:
  $$\text{Timestamp (UTC)} + \text{Log Level} + \text{Host Source} + \text{Message}$$
* A 64-bit **MurmurHash3 fingerprint** is computed across these fields.
* **Exit route:** If the format cannot be identified or parsed, it is routed to `quarantine.log`.

---

#### Stage 3: Sharded Deduplication (Lock-Striping)
Instead of locking the entire database, the fingerprint is routed to one of 16 independent lock buckets:

```text
Fingerprint: 0x9F4B2... ──► modulo 16 ──► Shard #7 [ Mutex Lock ]
                                                  │
                       ┌──────────────────────────┴──────────────────────────┐
                       ▼                                                     ▼
                  Already Seen?                                          New Entry?
                       │                                                     │
               Drop & Increment                                      Insert into Hash Set
            "duplicates_dropped"                                             │
                                                                             ▼
                                                                     Push to Queue 3
```

---

#### Stage 4: Sequential Persistence
* A single dedicated writer pulls clean records from **Queue 3**.
* Writes linearly to disk in append mode, avoiding disk-head thrashing and thread contention on file descriptors.

---

### 3. The Accounting Rule (Zero Record Loss)

Every single incoming line is accounted for at all times:

```text
Total Ingestion Count
         │
         ├──► 📁 quarantine.log         (Malformed / Incomplete)
         ├──► 🗑️ Duplicates Dropped      (Redundant records)
         └──► 📁 normalized_output.log   (Clean, unique data)
```

$$\text{Total Lines In} = \text{Written to Output} + \text{Duplicates Dropped} + \text{Quarantined Lines}$$

---

### 4. How Backpressure Works (Preventing OOM Crashes)

If the disk writer becomes slow (e.g., during an I/O spike), backpressure cascades backwards in reverse:

```text
[ Disk Slow ] 
      │
      ▼
Queue 3 Fills Up ──► Dedup Blocks ──► Parsers Block ──► Queue 1 Fills Up
                                                               │
                                                               ▼
                                                     Receiver Stops read()
                                                               │
                                                               ▼
                                                  OS TCP Window drops to 0
                                                               │
                                                               ▼
                                                 [ Remote Sender Pauses ]
```

* **No internal queue drops** occur. 
* Memory usage remains strictly bounded because the remote senders are forced to pause via standard **TCP flow control**.