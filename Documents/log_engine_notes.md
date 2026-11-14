# Log Processing & Analytics Engine: Project Notes

## 1. What we're building

A server (in C++, no Kafka or external tools) that:

- receives logs **in real time** from multiple sources
- handles logs in **different formats** (varying columns, messy, duplicated)
- **normalizes** them into one fixed schema
- **removes duplicates**
- writes everything into a **single output file**

**Main focus: concurrency.**

## 2. Difficulty

| Level | Verdict |
|---|---|
| Working version | Moderate (a few weeks for one engineer by hand) |
| Correct, fast, crash-safe version | Hard |

The difficulty is in the concurrency details, not the core idea. Since the code will be vibe coded, the real skill is **understanding the concepts well enough to give precise instructions and spot wrong answers**. Concurrency bugs don't crash loudly; they show up as lost logs, corrupted output, or hangs.

## 3. Workflow (plain words)

Think of a factory assembly line:

```
Sources → Receiver → [queue] → Parsers → [queue] → Dedup → [queue] → Writer → output file
```

1. Many machines send logs to the server.
2. The **receiver** collects raw bytes and cuts them into individual lines.
3. Lines wait in a **queue** so the receiver never waits on slow workers.
4. **Parsers** convert each messy line into one standard shape.
5. **Dedup** throws away logs already seen.
6. One **writer** appends clean logs to the output file.
7. On shutdown, everything is drained and flushed so nothing is lost.

## 4. The 5 core concepts

**1. Threads + mutex**
Threads do work in parallel (mainly parsing). A mutex ensures two threads never modify the same data at once.

**2. Bounded queue** (the most important building block)
A waiting line between stages with a size limit. If the next stage is slow, the queue fills and the previous stage waits instead of piling up memory. This gives you **backpressure** for free.

**3. Framing**
Network data is a stream of bytes, not neat lines. Collect bytes and split on `\n` to get complete logs. Skipping this cuts logs in half.

**4. Parsing and normalization**
Each format gets a small parser that outputs the same structure (timestamp, host, level, message). Parsers share nothing, so they run in parallel without locks.

**5. Dedup by hashing**
Turn each log into a small fingerprint (hash) and check whether it was seen before. To avoid locks, split fingerprints across a few **shards**, where each shard is owned by exactly one thread.

### Two supporting ideas

- **Single writer:** only one thread writes to the output file, so lines never get mixed up.
- **Graceful shutdown:** send an "end" signal through the queues so every stage flushes before exit.

## 5. Core subjects (just 4)

| # | Subject | What you need from it |
|---|---|---|
| 1 | **Operating Systems** | Threads, mutexes, condition variables, file I/O. This is where concurrency lives. |
| 2 | **Data Structures & Algorithms** | Bounded queue, hash functions, hash sets. |
| 3 | **Computer Networks** | Sockets and TCP framing only. |
| 4 | **Parsing & Text Processing** | Splitting lines into fields, mapping to one schema. Tedious, not hard. |

**Learning priority:** Operating Systems → Data Structures → Networks → Parsing.
If only one: start with threads, mutexes, and condition variables.

## 6. Running it on a laptop

- Check core count with `std::thread::hardware_concurrency()` (or `nproc` on Linux). Most laptops have 4 to 16 hardware threads.
- Rule of thumb: **total busy threads ≈ number of cores**. More just adds context-switching cost.
- Make thread counts configurable instead of hardcoding them.

Example split for an 8-thread laptop:

| Stage | Threads |
|---|---|
| Receiver | 1-2 |
| Parsers | 3-4 |
| Dedup shards | 2 |
| Writer | 1 (always) |

Laptop realities:

- Test senders run on the same machine and steal CPU, so measured throughput is lower than true capacity.
- Laptops throttle under sustained load, so run longer tests for honest numbers.
- RAM is limited, so keep queues and dedup memory **bounded**.
- Realistic target: hundreds of thousands of logs/sec, plenty to expose every real concurrency problem.

## 7. Build in stages

1. Single-threaded: one source, one format, one output file.
2. Add multiple formats and normalization.
3. Add concurrent ingestion with a bounded queue and a single writer.
4. Add parallel parsing workers.
5. Add sharded dedup.
6. Add counters and metrics (logs/sec, queue depth).

Keep each stage working before moving to the next.

## 8. How to verify it works

- Build with `-fsanitize=thread` (finds data races) and `-fsanitize=address,undefined` (memory bugs) from day one.
- **Stress test:** many fake senders send a known set of logs, including duplicates. Output must contain exactly the unique ones: nothing lost, nothing doubled.
- **Counters:** `received = written + deduped + rejected`. If this doesn't balance, there's a bug.
- Ask the AI to explain why every mutex or atomic exists. If it can't justify one, it's probably unnecessary or wrong.

## 9. Common C++ traps (to watch for in generated code)

- **Data races** are undefined behavior, not just wrong values.
- Always wait on a `condition_variable` **with a predicate** (avoids lost wakeups).
- Lock multiple mutexes in a consistent order (avoids deadlock).
- Move data through queues (`std::move`) instead of sharing pointers (avoids lifetime bugs).
- Tell the AI: **use mutexes first**; only go lock-free if measurements demand it.

## 10. Skip for now (optional polish later)

epoll, write-ahead log, watermarks and strict time ordering, Bloom filters, atomics and memory ordering, cache and false-sharing tuning.

## 11. Decisions to settle before generating code

1. **Does output order matter?** "Roughly arrival order" is simple. Strict time-sorted output needs a reorder buffer with a watermark, which is the hardest part of the project.
2. **What counts as a duplicate?** Exact same line, or same event with different ingest timestamps?
3. **What does "fixed number of rows" mean?** Most likely a fixed set of columns (fixed schema). If it means fixed-size batches or chunks, the writer design changes.
4. **Transport:** TCP (reliable, simplest to start) or UDP (faster, can lose data).

## 12. Suggested next step

Learn the **bounded queue** in detail (mutex + two condition variables: "not full" and "not empty"). Every other stage plugs into it.
