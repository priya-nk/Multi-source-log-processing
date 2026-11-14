#!/usr/bin/env python3
"""Generate concurrent newline-delimited JSON, Syslog, and Apache TCP logs."""

import argparse
import json
import socket
import threading
import time
from datetime import datetime, timezone
from pathlib import Path


def level_for(sequence):
    # Weighted distribution: routine events dominate, with regular warnings,
    # errors, and occasional critical events for a visible severity mix.
    slot = sequence % 20
    if slot in (0, 1):
        return "DEBUG"
    if slot <= 11:
        return "INFO"
    if slot <= 15:
        return "WARN"
    if slot <= 18:
        return "ERROR"
    return "CRITICAL"


def build_line(source_id, sequence, fmt):
    now = datetime.now(timezone.utc)
    stamp = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    level = level_for(sequence)
    request_id = f"req-{source_id[-1]}-{sequence:06d}"
    if fmt == "json":
        if level == "DEBUG":
            message = f"cache lookup request_id={request_id} key=customer:{sequence % 10000} hit=true"
        elif level == "INFO":
            message = (f"request completed request_id={request_id} method=GET "
                       f"path=/api/v1/orders/{sequence % 5000} status=200 duration_ms={4 + sequence % 180}")
        elif level == "WARN":
            message = f"rate limit reached request_id={request_id} client=198.51.100.{sequence % 250 + 1} retry_after=30s"
        elif level == "ERROR":
            message = f"database query failed request_id={request_id} database=orders error=connection_timeout"
        else:
            message = f"service health critical request_id={request_id} component=database consecutive_failures=5"
        return json.dumps({"timestamp": stamp, "level": level, "host": source_id,
                           "message": message}, separators=(",", ":"))
    if fmt == "syslog":
        # RFC 3164 timestamp has no timezone field; this simulated host uses UTC.
        timestamp_rfc3164 = f"{now.strftime('%b')} {now.day:2d} {now.strftime('%H:%M:%S')}"
        severity = {"EMERG": 0, "ALERT": 1, "CRITICAL": 2, "ERROR": 3,
                    "WARN": 4, "NOTICE": 5, "INFO": 6, "DEBUG": 7}[level]
        priority = 10 * 8 + severity  # authpriv facility
        pid = 2100 + sequence % 700
        if level == "DEBUG":
            message = f"sshd[{pid}]: accepted publickey user=demo from 198.51.100.{sequence % 250 + 1}"
        elif level == "INFO":
            message = f"systemd[{pid}]: Started scheduled maintenance task"
        elif level == "WARN":
            message = f"sshd[{pid}]: authentication failure user=admin from 203.0.113.{sequence % 250 + 1}"
        elif level == "ERROR":
            message = f"sshd[{pid}]: failed password user=admin from 203.0.113.{sequence % 250 + 1} port {30000 + sequence % 30000}"
        else:
            message = f"sshd[{pid}]: repeated authentication failures; temporarily blocking 203.0.113.{sequence % 250 + 1}"
        return f"<{priority}>{timestamp_rfc3164} {source_id} {message}"

    status = {0: 500, 1: 503, 2: 429, 3: 404, 4: 403, 5: 301, 6: 201}.get(sequence % 20, 200)
    methods = ("GET", "POST", "GET", "PUT", "DELETE")
    paths = ("/api/v1/orders", "/api/v1/login", "/health", "/static/app.js", "/api/v1/users")
    client_ip = f"192.0.2.{(sequence % 250) + 1}"
    apache_timestamp = now.strftime("%d/%b/%Y:%H:%M:%S +0000")
    method = methods[sequence % len(methods)]
    path = paths[sequence % len(paths)]
    return (f'{client_ip} - - [{apache_timestamp}] "{method} {path} HTTP/1.1" '
            f"{status} {128 + sequence % 2048}")


class RawFileSink:
    def __init__(self, path, mode):
        self.path = path
        self.file = path.open(mode, encoding="utf-8", newline="\n")
        self.lock = threading.Lock()

    def write_line(self, line):
        with self.lock:
            self.file.write(line + "\n")
            self.file.flush()

    def close(self):
        self.file.close()


def source_worker(args, source_index, errors, raw_sinks):
    source_id = f"source-{source_index + 1}"
    formats = ["json", "syslog", "apache"]
    fmt = formats[source_index % len(formats)] if args.format == "mixed" else args.format
    period = 1.0 / args.rate
    end_time = time.monotonic() + args.duration if args.duration else None
    sequence = 0
    previous_line = None
    try:
        with socket.create_connection((args.host, args.port), timeout=10) as sock:
            sock.settimeout(None)
            next_send = time.monotonic()
            while end_time is None or time.monotonic() < end_time:
                sequence += 1
                if args.malformed_every and sequence % args.malformed_every == 0:
                    line = '{"timestamp":broken,"message":}'
                elif args.duplicates_every and sequence % args.duplicates_every == 0 and previous_line:
                    line = previous_line
                else:
                    line = build_line(source_id, sequence, fmt)
                    previous_line = line
                sock.sendall(line.encode("utf-8") + b"\n")
                if raw_sinks:
                    raw_sinks[fmt].write_line(line)
                next_send += period
                delay = next_send - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
    except (OSError, BrokenPipeError) as exc:
        errors.append(f"{source_id}: {exc}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--sources", type=int, default=3)
    parser.add_argument("--rate", type=float, default=10.0,
                        help="lines per second per source")
    parser.add_argument("--duration", type=float, default=30.0,
                        help="seconds; use 0 to run until Ctrl+C")
    parser.add_argument("--format", choices=("mixed", "json", "syslog", "apache"), default="mixed")
    parser.add_argument("--duplicates-every", type=int, default=20,
                        help="repeat the previous line every N records; 0 disables")
    parser.add_argument("--malformed-every", type=int, default=50,
                        help="send malformed JSON every N records; 0 disables")
    parser.add_argument("--raw-dir", default="raw_logs",
                        help="directory for original raw lines, grouped by format")
    parser.add_argument("--raw-mode", choices=("overwrite", "append"), default="overwrite",
                        help="overwrite each used format file at startup or append to it")
    parser.add_argument("--no-raw-files", action="store_true",
                        help="do not save copies of lines sent to the TCP server")
    args = parser.parse_args()
    if args.sources < 1 or args.rate <= 0 or args.duration < 0:
        parser.error("sources and rate must be positive; duration cannot be negative")

    errors = []
    raw_sinks = {}
    if not args.no_raw_files:
        raw_dir = Path(args.raw_dir)
        raw_dir.mkdir(parents=True, exist_ok=True)
        formats = ("json", "syslog", "apache") if args.format == "mixed" else (args.format,)
        mode = "w" if args.raw_mode == "overwrite" else "a"
        suffix = {"json": "json_original.log", "syslog": "syslog_original.log",
                  "apache": "apache_original.log"}
        for fmt in formats:
            path = raw_dir / suffix[fmt]
            raw_sinks[fmt] = RawFileSink(path, mode)
            print(f"Raw {fmt} lines: {path}")

    threads = [threading.Thread(target=source_worker, args=(args, i, errors, raw_sinks), daemon=True)
               for i in range(args.sources)]
    print(f"Starting {args.sources} TCP sources -> {args.host}:{args.port}; "
          f"format={args.format}, rate={args.rate:g} lines/sec/source")
    for thread in threads:
        thread.start()
    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("Stopping source generators...")
    for thread in threads:
        if thread.is_alive():
            thread.join(timeout=1)
    for sink in raw_sinks.values():
        sink.close()
    for error in errors:
        print(f"source error: {error}")


if __name__ == "__main__":
    main()
