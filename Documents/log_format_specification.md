# Log Format Specification

## 1. The Original Format (Raw Input from Clients)

As discussed, the system accepts three completely different raw text formats depending on the client source:

*   **JSON Format:** Structured with explicit keys.
    $$\text{\{}\text{"timestamp": "...", "level": "...", "ip": "...", "message": "..."}\text{\}}$$
*   **Syslog Format:** Positional space and angle-bracket layout.
    $$\text{<Priority>Timestamp Hostname ProcessID Message}$$
*   **Apache Format:** Space-separated text lines with quotes.
    $$\text{IP-address - User [Timestamp] "Method URI Protocol" Status Bytes}$$

---

## 2. How It Is Stored in Your System (`normalized_output.log`)

Once the Parser Pool normalizes these varying inputs, the engine strips away their messy original syntax and saves them into a uniform, standardized layout in `normalized_output.log`.

Using our `LogEntry` structure, every single log—whether it originally came from JSON, Syslog, or Apache—is serialized into a clean, consistent format like this:

```text
[2026-11-10T12:00:00Z] | [ERROR] | [192.168.1.50] | Database connection timeout
[2026-11-10T12:00:01Z] | [INFO]  | [127.0.0.1]    | User authentication successful
[2026-11-10T12:00:02Z] | [WARN]  | [10.0.0.15]    | High memory usage detected (85%)
```

### Why this structure?

By converting messy, heterogeneous logs into a clean, uniform format:
*   **Instant Querying:** Downstream queries become instant and reliable.
*   **Efficient I/O:** The Disk Writer can append lines sequentially without worrying about missing fields or changing syntax.