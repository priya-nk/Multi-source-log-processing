#!/usr/bin/env python3
"""Serve a small local browser view of the pipeline's normalized CSV output."""

import argparse
import csv
import io
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse


PAGE = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Live Log Output</title>
  <style>
    :root { color-scheme: dark; font-family: Inter, Segoe UI, sans-serif; background: #10151d; color: #e7edf5; }
    * { box-sizing: border-box; }
    body { margin: 0; padding: 32px; }
    main { max-width: 1280px; margin: auto; }
    header { display: flex; align-items: end; justify-content: space-between; gap: 20px; margin-bottom: 24px; }
    h1 { margin: 0 0 7px; font-size: clamp(25px, 4vw, 36px); letter-spacing: -.04em; }
    .subtitle, .muted { color: #93a2b5; }
    .subtitle { margin: 0; }
    .status { display: flex; align-items: center; gap: 9px; color: #b9c6d7; font-size: 14px; white-space: nowrap; }
    .dot { width: 9px; height: 9px; border-radius: 50%; background: #51d69a; box-shadow: 0 0 14px #51d69a80; }
    .dot.error { background: #ff6b75; box-shadow: 0 0 14px #ff6b7580; }
    .summary { display: flex; align-items: center; justify-content: space-between; gap: 16px; margin-bottom: 14px; padding: 14px 17px; background: #171f2a; border: 1px solid #293647; border-radius: 12px; }
    .summary strong { color: #fff; font-size: 19px; }
    .summary span { color: #9cacbf; font-size: 13px; }
    .table-wrap { overflow: auto; border: 1px solid #293647; border-radius: 12px; background: #141b24; }
    table { width: 100%; border-collapse: collapse; min-width: 760px; }
    th { position: sticky; top: 0; background: #202b39; color: #b8c8da; text-align: left; font-size: 12px; text-transform: uppercase; letter-spacing: .08em; }
    th, td { padding: 12px 14px; border-bottom: 1px solid #263241; vertical-align: top; }
    td { font-size: 13px; line-height: 1.45; overflow-wrap: anywhere; }
    tr:last-child td { border-bottom: 0; }
    td.time { white-space: nowrap; color: #aab8ca; }
    td.level { width: 90px; font-weight: 700; color: #8bc7ff; }
    td.source { color: #c0a7ff; }
    td.message { white-space: pre-wrap; }
    .empty { padding: 48px 20px; text-align: center; color: #93a2b5; }
    @media (max-width: 650px) { body { padding: 18px 12px; } header { align-items: start; flex-direction: column; } }
  </style>
</head>
<body>
<main>
  <header>
    <div><h1>Live Log Output</h1><p class="subtitle">Normalized records from the pipeline output file</p></div>
    <div class="status"><span id="dot" class="dot"></span><span id="status">Connecting…</span></div>
  </header>
  <section class="summary"><strong id="total">0 records</strong><span id="updated">Waiting for output</span></section>
  <section class="table-wrap">
    <table>
      <thead><tr><th>Timestamp</th><th>Level</th><th>Host / Source</th><th>Message</th></tr></thead>
      <tbody id="rows"><tr><td colspan="4" class="empty">Waiting for the first parsed records…</td></tr></tbody>
    </table>
  </section>
</main>
<script>
const body = document.getElementById('rows');
const statusText = document.getElementById('status');
const dot = document.getElementById('dot');
async function refresh() {
  try {
    const response = await fetch('/api/logs', {cache: 'no-store'});
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    body.replaceChildren();
    if (data.rows.length === 0) {
      const row = document.createElement('tr');
      const cell = document.createElement('td');
      cell.colSpan = 4; cell.className = 'empty';
      cell.textContent = 'No parsed records yet. Start the server and log generators.';
      row.appendChild(cell); body.appendChild(row);
    } else {
      for (const record of data.rows) {
        const row = document.createElement('tr');
        for (const [value, className] of [[record.timestamp, 'time'], [record.log_level, 'level'], [record.host_source, 'source'], [record.message, 'message']]) {
          const cell = document.createElement('td');
          cell.className = className;
          cell.textContent = value;
          row.appendChild(cell);
        }
        body.appendChild(row);
      }
    }
    document.getElementById('total').textContent = `Showing ${data.total.toLocaleString()} latest rows`;
    document.getElementById('updated').textContent = `Updated ${new Date().toLocaleTimeString()} · showing latest ${data.rows.length}`;
    statusText.textContent = 'Connected · refreshes every 750 ms';
    dot.classList.remove('error');
  } catch (error) {
    statusText.textContent = `Cannot read output: ${error.message}`;
    dot.classList.add('error');
  }
}
refresh();
setInterval(refresh, 750);
</script>
</body>
</html>"""


def read_latest_rows(path: Path, limit: int):
    if not path.exists():
        return [], 0
    size = path.stat().st_size
    # A normalized row is bounded by LogEntry's fixed field sizes. Read a
    # generous tail window so the dashboard doesn't reload a huge log file.
    max_bytes = max(256 * 1024, limit * 2048)
    with path.open("rb") as file:
        file.seek(max(0, size - max_bytes))
        data = file.read()
    if not data.endswith(b"\n"):
        end = data.rfind(b"\n")
        data = data[:end + 1] if end >= 0 else b""
    if size > max_bytes:
        first_newline = data.find(b"\n")
        data = data[first_newline + 1:] if first_newline >= 0 else b""

    text = data.decode("utf-8", errors="replace")
    try:
        parsed = list(csv.reader(io.StringIO(text, newline="")))
    except csv.Error:
        parsed = []
    records = [row for row in parsed if len(row) == 4 and row[0] != "timestamp"]
    records = records[-limit:]
    result = [{"timestamp": r[0], "log_level": r[1], "host_source": r[2], "message": r[3]}
              for r in reversed(records)]
    return result, len(records)


def make_handler(output_path: Path, row_limit: int):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            route = urlparse(self.path).path
            if route == "/":
                payload = PAGE.encode("utf-8")
                content_type = "text/html; charset=utf-8"
            elif route == "/api/logs":
                rows, total = read_latest_rows(output_path, row_limit)
                payload = json.dumps({"rows": rows, "total": total}).encode("utf-8")
                content_type = "application/json; charset=utf-8"
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, fmt, *args):
            # Keep the dashboard terminal quiet while the browser polls.
            pass

    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", default="normalized_output.csv", help="CSV file written by the C++ pipeline")
    parser.add_argument("--host", default="127.0.0.1", help="bind address; loopback only by default")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--rows", type=int, default=500, help="maximum recent rows shown")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535 or args.rows < 1:
        parser.error("port must be 1..65535 and rows must be positive")
    output_path = Path(args.file).expanduser().resolve()
    server = ThreadingHTTPServer((args.host, args.port), make_handler(output_path, args.rows))
    print(f"Dashboard: http://{args.host}:{args.port}/")
    print(f"Watching: {output_path}")
    try:
        server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        print("Stopping dashboard")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
