# Byte-Stream Framing Algorithm

## 1. What is the Byte-Stream Framing Algorithm?

The **Byte-Stream Framing Algorithm** (implemented in `Receiver::process_bytes()`) is the mechanism used in network programming to carve discrete messages or lines out of a continuous, unstructured stream of bytes arriving over TCP.

### The Problem It Solves

TCP is a stream-oriented protocol, not a message-oriented one. This means TCP does not care about your application's boundaries. If a client sends three log lines, the operating system kernel might deliver them in entirely unpredictable chunks:

* All three lines might arrive glued together in a single `recv()` call.
* A single long log line might get cut in half across two separate `recv()` calls (e.g., the first half arrives now, and the rest arrives a few milliseconds later).

### How the Algorithm Works

To solve this, the Receiver maintains an in-memory scratchpad called a **Sliding Linear Byte Buffer** (`std::vector<char>`) and runs the framing algorithm:

1. **Append:** As raw bytes arrive from `recv()`, they are appended to the end of the sliding buffer.
2. **Scan:** The algorithm scans the buffer linearly from left to right looking for the message delimiter—in this case, the newline character (`\n`).
3. **Extract (Frame):** Every time a `\n` is found, everything from the start of the line up to that `\n` is sliced out as a complete, discrete log string and pushed immediately into Queue 1.
4. **Slide / Shift:** Once the complete lines are extracted, any remaining unparsed bytes (an incomplete tail fragment) are shifted back to index `0` of the buffer so they can be completed when the next TCP packet arrives.
5. **Disconnect Safety:** If a client abruptly disconnects mid-stream and leaves behind an incomplete trailing fragment without a newline, that leftover fragment is captured and routed to `quarantine.log` with the metadata reason `incomplete_frame_connection_reset`.

---

## 2. Is This a Standard Algorithm?

**Yes and no**—it is a standard pattern/concept, but it is not a single named "textbook" algorithm.

* **The Concept is Universal:** Every networking library, server framework, and protocol parser (like HTTP parsers, Redis protocol decoders, or Kafka brokers) must solve this exact issue. Any application reading from a TCP socket must implement a framing/buffering loop because TCP raw streams have no native message boundaries.
* **Why it's custom code here:** While the pattern is standard, you won't find a built-in `stream_framer()` function in the C++ standard library. Developers implement it manually using standard containers (`std::vector<char>` or string buffers) tailored to their specific delimiter (such as `\n`, null-terminators, or length-prefixed headers).