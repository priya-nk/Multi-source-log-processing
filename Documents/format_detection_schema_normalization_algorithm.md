# Format Detection & Schema Normalization Algorithm

## 1. What is the Format Detection & Schema Normalization Algorithm?

The **Format Detection and Schema Normalization Algorithm** (executed inside `ParserPool::worker_loop()`) is the second stage of the pipeline. Its job is to take raw, unstructured, or heterogeneously formatted log strings arriving from Queue 1 and transform them into a unified, canonical `LogEntry` structure.

### The Problem It Solves

In real-world environments, different microservices, servers, and applications write logs in completely different formats (e.g., JSON payloads, traditional Syslog formats, Apache web server logs, or custom key-value pairs). Downstream processing and analytics engines cannot easily query raw, chaotic text strings. This algorithm inspects each log line, detects its format dynamically, extracts its core fields, and standardizes them.

---

## 2. Step-by-Step Execution Flow

1. **Pop from Queue 1:** A parser worker thread pops a raw, framed log string out of Queue 1.
2. **Signature-Matching Heuristics:** The algorithm runs fast, prefix-checking scans to determine the log's format in a specific order:
   * **JSON Logs:** Scans for enclosing `{ ... }` curly braces and extracts keys like `timestamp`, `level`, `host`, and `msg`.
   * **Syslog (RFC 3164 / 5424):** Scans for priority tags (e.g., `<PRI>`), standard timestamp structures, and facility names.
   * **Apache Common/Combined:** Extracts client IP addresses, authentication tokens, timestamp brackets (`[...]`), and HTTP request lines.
   * **Key-Value Pairs:** Splits lines by whitespace and key-delimiter tokens (`key=value`).
3. **Timestamp Standardization:** Whichever format is detected, the algorithm converts the log's original timestamp into a standard ISO-8601 UTC string.
4. **Construct Canonical Struct:** It bundles the normalized fields into the unified `LogEntry` memory layout (`timestamp`, `log_level`, `host_source`, `message`, and the precomputed fingerprint).
5. **Quarantine Fallback:** If a log line does not match any recognized signature or is structurally corrupt, it bypasses the deduplication stage entirely and appends directly to `quarantine.log` with the metadata reason `"unknown_format"`.

---

## 3. Why Heavy Regular Expressions (`std::regex`) Are Avoided

A common naive approach to log parsing is writing complex regular expressions (`std::regex`). However, the engine deliberately avoids `std::regex` because it introduces severe performance bottlenecks.

Instead, the algorithm relies on fast manual string scanning, prefix checks, and string-view algorithms. Hand-written string parsers operate orders of magnitude faster, avoiding the heavy backtracking overhead of general-purpose regex engines and ensuring the parser pool can keep pace with high-throughput network streams.


# Difference
- No, signature-matching heuristics are not regular expressions (regex), even though both are used to recognize patterns in text.
They represent two completely different approaches to solving the same problem:
1. What Signature-Matching Heuristics Actually Are
In a high-performance engine, signature matching means using lightweight, manual string checks—essentially looking for cheap "tells" or fixed structural clues using basic C++ string functions (std::string_view, find(), at(), or pointer arithmetic).
For example, checking a log line's format takes just a few micro-operations:
- JSON Check: if (!line.empty() && line.front() == '{' && line.back() == '}') $\rightarrow$ Instantly flags it as JSON without checking any middle characters.
- Syslog Check: if (line.front() == '<' && line.find('>') != std::string::npos) $\rightarrow$ Looks for the priority angle brackets.
- Apache Check: Looks for specific quotation marks ("GET ... ") or space-separated token counts at fixed positions.
These checks involve zero backtracking and run in straight-line machine instructions.
2. How They Differ from Regular Expressions (std::regex)
A regular expression engine (like std::regex in C++) is a heavy, generalized parser. When you pass a regex pattern, the engine:
- Compiles the pattern into a complex Finite State Machine (FSM).
- Executes matching logic that handles wildcards (.*), optional characters (?), quantifiers (+), and capturing groups ((...)).
- Often performs backtracking—meaning if a pattern match fails halfway through a string, the CPU loops backward to try alternative matching paths.
This backtracking and state-machine overhead is precisely why standard regex engines are notoriously slow and why high-performance systems avoid them.

### Summary
- Regex: A generalized, highly flexible pattern-matching language that uses state machines and backtracking, which kills CPU performance at scale.
- Heuristics: Fast, hand-written C++ prefix and character checks (starts_with, find, indexing) designed to instantly categorize a string with minimal CPU cycles.
